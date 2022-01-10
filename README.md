# node-red-contrib-snowflakedw

[node-red-contrib-snowflakedw](https://github.com/mnestor/node-red-contrib-snowflakedw)
is a [**Node-RED**](https://nodered.org/) node to query a [**Snowflake**](https://www.snowflake.com/) 🐘 database using their [Node.js driver](https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html)

It supports *splitting* the resultset and *backpressure* (flow control), to allow working with large datasets.

It supports *parameterized queries*

## Outputs

The response (rows) is provided in `msg.payload` as an array.

Additional information is provided as `msg.sql.rowCount` and `msg.pgsqlsql.command`.

## Inputs

### SQL query template

This node uses the [Mustache template system](https://github.com/janl/mustache.js) to generate queries based on the message:

```sql
-- INTEGER id column
SELECT * FROM table WHERE id = {{{ msg.id }}};

-- TEXT id column
SELECT * FROM table WHERE id = '{{{ msg.id }}}';
```

### Dynamic SQL queries

As an alternative to using the query template above, this node also accepts an SQL query via the `msg.query` parameter.

### Parameterized query (numeric)

Parameters for parameterized queries can be passed as a parameter array `msg.params`:

```js
// In a function, provide parameters for the parameterized query
msg.params = [ msg.id, 3 ];
```

```sql
-- In this node, use a parameterized query
SELECT * FROM table WHERE id = :1 OR id = :2;
```

## Installation

### Using the Node-RED Editor

You can install [**node-red-contrib-snowflakedw**](https://flows.nodered.org/node/node-red-contrib-snowflakedw) directly using the editor:
Select *Manage Palette* from the menu (top right), and then select the *Install* tab in the palette.

### Using npm

You can alternatively install the [npm-packaged node](https://www.npmjs.com/package/node-red-contrib-snowflakedw):

* Locally within your user data directory (by default, `$HOME/.node-red`):

```sh
cd $HOME/.node-red
npm i node-red-contrib-snowflakedw
```

* or globally alongside Node-RED:

```sh
npm i -g node-red-contrib-snowflakedw
```

You will then need to restart Node-RED.

## Backpressure

This node supports *backpressure* / *flow control*:
when the *Split results* option is enabled, it waits for a *tick* before releasing the next batch of lines,
to make sure the rest of your Node-RED flow is ready to process more data
(instead of risking an out-of-memory condition), and also conveys this information upstream.

So when the *Split results* option is enabled, this node will only output one message at first,
and then awaits a message containing a truthy `msg.tick` before releasing the next message.

To make this behaviour potentially automatic (avoiding manual wires), this node declares its ability by exposing a truthy `node.tickConsumer`
for downstream nodes to detect this feature, and a truthy `node.tickProvider` for upstream nodes.
Likewise, this node detects upstream nodes using the same back-pressure convention, and automatically sends ticks.

## Sequences for split results

When the *Split results* option is enabled (streaming), the messages contain some information following the
conventions for [*messages sequences*](https://nodered.org/docs/user-guide/messages#message-sequences).

```js
{
  payload: '...',
  parts: {
    id: 0.1234, // sequence ID, randomly generated (changes for every sequence)
    index: 5, // incremented for each message of the same sequence
    count: 6 // total number of messages; only available in the last message of a sequence
  },
  complete: true, // True only for the last message of a sequence
}
```

## Credits

Major credit due to [Alexandre Alapetite](https://alexandra.dk/alexandre.alapetite) ([Alexandra Institute](https://alexandra.dk)) as a good deal of the ideas for this implementation and even some copy/paste of his [https://github.com/alexandrainst/node-red-contrib-postgresql](https://github.com/alexandrainst/node-red-contrib-postgresql)

Contributions and collaboration welcome.
