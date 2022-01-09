"use strict";

/**
 * Return an incoming node ID if the node has any input wired to it, false otherwise.
 * If filter callback is not null, then this function filters incoming nodes.
 * @param {Object} toNode
 * @param {function} filter
 * @return {(number|boolean)}
 */
 function findInputNodeId(toNode, filter = null) {
	if (toNode && toNode._flow && toNode._flow.global) {
		const allNodes = toNode._flow.global.allNodes;
		for (const fromNodeId of Object.keys(allNodes)) {
			const fromNode = allNodes[fromNodeId];
			if (fromNode.wires) {
				for (const wireId of Object.keys(fromNode.wires)) {
					const wire = fromNode.wires[wireId];
					for (const toNodeId of wire) {
						if (toNode.id === toNodeId && (!filter || filter(fromNode))) {
							return fromNode.id;
						}
					}
				}
			}
		}
	}
	return false;
}

module.exports = function (RED) {
  const Mustache = require('mustache');
  const Snowflake = require("snowflake-sdk");
  const async = require("async");

	function getField(node, kind, value) {
		switch (kind) {
			case 'flow':	// Legacy
				return node.context().flow.get(value);
			case 'global':
				return node.context().global.get(value);
			case 'num':
				return parseInt(value);
			case 'bool':
				return JSON.parse(value);
			case 'env':
				return process.env[value];
			default:
				return value;
		}
	}

  function SnowflakeConfigNode(n) {
    const node = this;
    RED.nodes.createNode(node, n);

    node.name = n.name;
    node.account = n.account;
    node.accountFieldType = n.accountFieldType;
    node.username = n.username;
    node.usernameFieldType = n.usernameFieldType;

    // Normal user/pass
    node.password = n.password;
    node.passwordFieldType = n.passwordFieldType;

    // using a private key
    node.privateKey = n.privateKey;
    node.privateKeyFieldType = n.privateKeyFieldType;
    node.privateKeyPass = n.privateKeyPass;
    node.privateKeyPassFieldType = n.privateKeyPassFieldType;

    node.warehouse = n.warehouse;
    node.warehouseFieldType = n.warehouseFieldType;
    node.schema = n.schema;
    node.schemaFieldType = n.schemaFieldType;
    node.role = n.role;
    node.roleFieldType = n.roleFieldType;
    node.database = n.database;
    node.databaseFieldType = n.databaseFieldType;

    node.resetContext = true;
    node.retryWaitTime = 300;

    node.connectionInfo = {
      account: getField(node, n.accountFieldType, n.account),
      username: getField(node, n.usernameFieldType, n.username)
    };

    const privateKey = getField(node, n.privateKeyFieldType, n.privateKey);

    if (privateKey.length > 0) {
      node.connectionInfo["authenticator"] = "SNOWFLAKE_JWT";
      node.connectionInfo["privateKey"] = privateKey;
      node.connectionInfo["privateKeyPass"] = getField(node, n.privateKeyPassFieldType, n.privateKeyPass);
    } else {
      node.connectionInfo["password"] = getField(node, n.passwordFieldType, n.password);
    }

    node.getConnection = () => {
      node.resetContext = true;
      return Snowflake.createConnection(node.connectionInfo);
    }
    node.snowflakeConn = node.getConnection();

    node.snowflakeExecContext = {
      warehouse: getField(node, n.warehouseFieldType, n.warehouse),
      schema: getField(node, n.schemaFieldType, n.schema),
      role: getField(node, n.roleFieldType, n.role),
      database: getField(node, n.databaseFieldType, n.database),
    }

    // mostly stolen from @MadhuPolu
    // https://github.com/snowflakedb/snowflake-connector-nodejs/issues/168#issuecomment-980509649
    node.getEstablishedConnection = async () => {
      const resolvedActiveConnection = await new Promise((resolve, reject) => {
        (node.snowflakeConn.isUp()) || false
          ? resolve(node.snowflakeConn)
          : node.snowflakeConn.connect((err, _connection) => {
            if (err) {
              // console.error(err.message);
              if (
                err.code === 405501 ||
                err.message === "Connection already in progress."
              ) {
                setTimeout(() => {
                  resolve(node.getEstablishedConnection());
                }, node.retryWaitTime);
              } else if (
                err.code === 405503 ||
                err.message === "Connection already Terminated. Cannot connect again."
              ) {
                node.snowflakeConn = node.getConnection();
                setTimeout(() => {
                  resolve(node.getEstablishedConnection());
                }, 0);
              } else {
                reject(err);
              }
            }
            else {
              resolve(_connection);
            }
          });
      });

      if (node.resetContext) {
        // console.debug("Resetting context");
        node.resetContext = false;
        const statements = [
          {
            sqlText: `USE ROLE ${ node.snowflakeExecContext.role}`
          },{
            sqlText: `USE WAREHOUSE ${node.snowflakeExecContext.warehouse};`
          },{
            sqlText: `USE ${node.snowflakeExecContext.database}.${node.snowflakeExecContext.schema};`
          }
        ];

        for (const statement of statements) {
          await node.executeStmt(statement)
            .catch((err) => {
              reject(err)
            })
        }
      }

      return resolvedActiveConnection;
    }

    node.executeStmt = async (statement) => {
      console.info(statement);
      const connection = await node.getEstablishedConnection();
      return await new Promise(function(resolve, reject) {
        connection.execute({
          ...statement,
          complete: (err, stmt, rows) => {
            if (err) {
              if (
                err.code === 407002 ||
                err.message === "Unable to perform operation using terminated connection."
              ) {
                setTimeout(async () => {
                  resolve(await this.executeStmt(statement));
                }, node.retryWaitTime);
              } else {
                reject(err);
              }
            } else {
              resolve([stmt, rows]);
            }
          }
        })
      });
      // return results || [null, []];
    }

    node.close = async () => {
      let connection = await node.getEstablishedConnection();
      connection.destroy();
    }

  }
  RED.nodes.registerType("snowflakeSQLConfig", SnowflakeConfigNode);

  function SnowflakeSQLNode(config) {
    const node = this;
    RED.nodes.createNode(node, config);
		node.topic = config.topic;
		node.query = config.query;
		node.split = config.split;
		node.rowsPerMsg = config.rowsPerMsg;
    this.server = RED.nodes.getNode(config.snowflakeSQLConfig);

		// Declare the ability of this node to provide ticks upstream for back-pressure
		node.tickProvider = true;
		let tickUpstreamId;
		let tickUpstreamNode;

		// Declare the ability of this node to consume ticks from downstream for back-pressure
		node.tickConsumer = true;
		let downstreamReady = true;

		// For streaming from PostgreSQL
		let cursor;
		let getNextRows;

    node.status({});

    node.on('close', function() {
      node.server.close();
    });

    node.on('input', async (msg, send, done) => {
			if (tickUpstreamId === undefined) {
				tickUpstreamId = findInputNodeId(node, (n) => RED.nodes.getNode(n.id).tickConsumer);
				tickUpstreamNode = tickUpstreamId ? RED.nodes.getNode(tickUpstreamId) : null;

        // prevent looping, we won't sent back upstream if the previous node is the same
        if (tickUpstreamNode instanceof SnowflakeSQLNode) {
          tickUpstreamId = null;
          tickUpstreamNode = null;
        }
			}

			if (msg.tick) {
        // console.log('message ticking');
				downstreamReady = true;
				if (getNextRows) {
					getNextRows();
				}
			} else {
        const partsId = Math.random();
				let query = msg.query ? msg.query : Mustache.render(node.query, { msg });

				const handleError = (err) => {
					const error = (err ? err.toString() : 'Unknown error!') + ' ' + query;
					// handleDone();
					msg.payload = error;
					msg.parts = {
						id: partsId,
						abort: true,
					};
					downstreamReady = false;
					if (err) {
            node.error(err.message, msg);
						if (done) {
              done();
            }
					}
				};

				downstreamReady = true;

        const [stmt, rows] = await node.server.executeStmt({
          sqlText: query,
          binds: msg.binds || [],
          streamResult: node.split
        }).then((data) => {
          return data;
        }).catch((err) => {
          handleError(err);
          return [null, null];
        });

        if (stmt === null) {
          return;
        }

        if (node.split) {
          let partsIndex = 0;
          msg.sql = {
            command: query
          };

          let rows = [];
          const stream = stmt.streamRows();

          stream.on('error', (err) => {
            node.error(err.message, msg);
            done();
          });

          let readableRunning = false;

          stream.on('readable', () => {
            getNextRows = async () => {
              if (!downstreamReady) {
                return;
              }

              let nextData = null;
              let finished = false;
              for (let i = rows.length; i < node.rowsPerMsg; i++) {
                let data = await stream.read();
                if (data === null) {
                  finished = true;
                  break;
                }
                rows.push(data);
              }

              if (!finished) {
                nextData = await stream.read();
                if (nextData === null) {
                  finished = true;
                }
              }

              //clone this so we don't mess up the messages
              const msg2 = Object.assign({}, msg, {
                payload: rows,
                complete: nextData === null,
                parts: {
                  id: partsId,
                  type: 'array',
                  index: partsIndex
                }
              });

              if (finished) {
                msg2.parts['count'] = partsIndex++;
              }

              node.send(msg2);

              if (tickUpstreamNode) {
                tickUpstreamNode.receive({ tick: true });
              }

              rows = [];
              partsIndex++;
              downstreamReady = false;

              //we get a null on the last item.
              if (nextData !== null) {
                rows.push(nextData);
              } else {
                getNextRows = null;
                if (done) {
                  done();
                }
              }
            };

            // emit data on start of readable
            if (!readableRunning) {
              readableRunning = true;
              getNextRows();
            }
          });
        } else {
          msg.sql = {
            command: query,
            rowCount: rows.length
          };

          msg.payload = rows;
          downstreamReady = false;
          send(msg);

          if (tickUpstreamNode) {
            tickUpstreamNode.receive({ tick: true });
          }

          if (done) {
            done();
          }
        }
        // };

        // getNextRows();
      }
    });
  }

  RED.nodes.registerType("snowflakeSQL", SnowflakeSQLNode)
};
