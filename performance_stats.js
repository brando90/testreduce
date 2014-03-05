//var conn = new Connection({'host': host, 'port': port, 'keyspace': 'Keyspace1', 'cql_version': '2.0.0'});
//var pool = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1', 'cql_version': '2.0.0'});

var Connection = require('cassandra-client').Connection;
var con = new Connection({host:'cassandra-host', port:9160, keyspace: "testreducedb");
con.connect(function(err, con) {
  if (err) {
    // Failed to establish connection.
		console.log("Failed to establish connection BRANDO!");
    throw err;
  }

/*
	con.execute('UPDATE Standard1 SET ?=? WHERE key=?', ['cola', 'valuea', 'key0'], function(err) {
      if (err) {
          // handle error
      } else {
          // handle success.
      }
  });
*/
});




