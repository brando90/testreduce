cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['localhost'], keyspace: "testreducedb", username: "testreduce", password: ""});

client.connect( function(err, result) {
	if (err){
		console.log("connection failed");
	} else {
		console.log("connection succesful in the .connect() commands");
	}
}
);

client.execute('SELECT test FROM results', function(err, result) {
    if (err){
			console.log('execute failed');
			console.log(err);
			//throw err;
		} else {
			console.log('execute succesful');		
		}
}
);


