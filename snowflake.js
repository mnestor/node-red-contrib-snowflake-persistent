"use strict";

// const { resolve } = require('path/posix');

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

const initiateConnection = async (connection, node) => {
  return new Promise(function(resolve, reject) {
    if (!connection.isUp()) {
      connection.connect(async (err, conn) => {
        if (err) {
          node.status({fill:"red", shape:"ring", text: "Error"});
          reject(err);
        } else {
          const statements = [
            {
              sqlText: `USE ROLE ${ node.config.snowflakeExecContext.role}`
            },{
              sqlText: `USE WAREHOUSE ${node.config.snowflakeExecContext.warehouse};`
            },{
              sqlText: `USE ${node.config.snowflakeExecContext.database}.${node.config.snowflakeExecContext.schema};`
            }
          ];

          for (const statement of statements) {
            await executeStmt(connection, statement)
              .catch((err) => {
                reject(err)
              })
          }

          node.status({fill:"green", shape:"ring", text: "Connected"});
          resolve(connection);
        }
      });
    } else {
      node.status({fill:"green", shape:"ring", text: "Connected"});
      resolve(connection);
    }
  });
}

const executeStmt = async (connection, statement) => {
  return new Promise(function(resolve, reject) {
    connection.execute({
      ...statement,
      complete: (err, stmt) => {
        if (err) {
          reject(err);
        } else {
          resolve(true);
        }
      }
    })
  });
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

    let connectionInfo = {
      account: getField(node, n.accountFieldType, n.account),
      username: getField(node, n.usernameFieldType, n.username)
    };

    const privateKey = getField(node, n.privateKeyFieldType, n.privateKey);

    if (privateKey.length > 0) {
      connectionInfo["authenticator"] = "SNOWFLAKE_JWT";
      connectionInfo["privateKey"] = privateKey;
      connectionInfo["privateKeyPass"] = getField(node, n.privateKeyPassFieldType, n.privateKeyPass);
    } else {
      connectionInfo["password"] = getField(node, n.passwordFieldType, n.password);
    }

    this.snowflakeConn = Snowflake.createConnection(connectionInfo);

    this.snowflakeExecContext = {
      warehouse: getField(node, n.warehouseFieldType, n.warehouse),
      schema: getField(node, n.schemaFieldType, n.schema),
      role: getField(node, n.roleFieldType, n.role),
      database: getField(node, n.databaseFieldType, n.database),
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
    this.config = RED.nodes.getNode(config.snowflakeSQLConfig);

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
      node.config.snowflakeConn.destroy();
      node.status({fill:"green", shape:"ring", text: "Disconnected"});
    });

    node.on('input', async (msg, send, done) => {
			// 'send' and 'done' require Node-RED 1.0+
			send = send || function () { node.send.apply(node, arguments); };

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
        console.log('message ticking');
				downstreamReady = true;
				if (getNextRows) {
					getNextRows();
				}
			} else {
        const partsId = Math.random();
				let query = msg.query ? msg.query : Mustache.render(node.query, { msg });

				let connection = null;

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

				// handleDone();
				downstreamReady = true;

        try {
          connection = await initiateConnection(node.config.snowflakeConn, node)
            .catch((err) => {
              throw err;
            });

					let params = [];
					if (msg.params && msg.params.length > 0) {
						params = msg.params;
					} else if (msg.queryParameters && (typeof msg.queryParameters === 'object')) {
						({ text: query, values: params } = named.convert(query, msg.queryParameters));
					}

					// if (node.split) {
					// 	let partsIndex = 0;
					// 	delete msg.complete;

					// 	cursor = client.query(new Cursor(query, params));

					// 	const cursorcallback = (err, rows, result) => {
					// 		if (err) {
					// 			handleError(err);
					// 		} else {
					// 			const complete = rows.length < node.rowsPerMsg;
					// 			if (complete) {
					// 				handleDone();
					// 			}
					// 			const msg2 = Object.assign({}, msg, {
					// 				payload: (node.rowsPerMsg || 1) > 1 ? rows : rows[0],
					// 				pgsql: {
					// 					command: result.command,
					// 					rowCount: result.rowCount,
					// 				},
					// 				parts: {
					// 					id: partsId,
					// 					type: 'array',
					// 					index: partsIndex,
					// 				},
					// 			});
					// 			if (msg.parts) {
					// 				msg2.parts.parts = msg.parts;
					// 			}
					// 			if (complete) {
					// 				msg2.parts.count = partsIndex + 1;
					// 				msg2.complete = true;
					// 			}
					// 			partsIndex++;
					// 			downstreamReady = false;
					// 			send(msg2);
					// 			if (complete) {
					// 				if (tickUpstreamNode) {
					// 					tickUpstreamNode.receive({ tick: true });
					// 				}
					// 				if (done) {
					// 					done();
					// 				}
					// 			} else {
					// 				getNextRows();
					// 			}
					// 		}
					// 	};

					// 	getNextRows = () => {
					// 		if (downstreamReady) {
					// 			cursor.read(node.rowsPerMsg || 1, cursorcallback);
					// 		}
					// 	};
					// } else {
						getNextRows = async () => {
							try {
								await connection.execute({
                  sqlText: query,
                  streamResult: false,
                  complete: async (err, stmt, rows) => {
                    try {
                      msg.payload = rows;
                      msg.sql = {
                        command: query,
                        rowCount: rows.length
                      };
                      downstreamReady = false;
                      send(msg);
                      if (tickUpstreamNode) {
                        tickUpstreamNode.receive({ tick: true });
                      }
                      if (done) {
                        done();
                      }
                    }
                    catch (err) {
                      node.error(err.message, msg);
                      done();
                    }
                  }
                });
							} catch (ex) {
								handleError(ex);
							}
						};
					// }

					getNextRows();
				} catch (err) {
					handleError(err);
				}
      }
    });
  }

  RED.nodes.registerType("snowflakeSQL", SnowflakeSQLNode)
};
