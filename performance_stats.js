cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['localhost'], keyspace: "testreducedb", username: "testreduce", password: "testreduce"});

//testing connection
client.connect( function(err, result) {
	if (err){
		console.log("connection failed");
	} else {
		console.log("connection succesful in the .connect() commands");
	}
}
);

/*
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
*/

/*
var createTestBlob = function(prefix, title) {
    return new Buffer(JSON.stringify({prefix: prefix, title:title, oldid:42}));
};
*/

var insertTimeScore = function(commit, pageID, time) {
    console.log('insert called');
    var query = 'INSERT INTO page_by_time_score (commit, pageID) VALUES (?,?,?);';
    client.execute(query, [commit, pageID, time], 1, function(err, result) {
        if (err) {
            console.log(err);
        } else {
        }
    });
};

var insertBySizeScore = function(commit, pageID, size) {
    var query = "INSERT INTO test_by_size_score (commit, pageID, size) VALUES (?, ?, ?);",
    client.execute(query, [commit, pageID, size], 1, function(err, result) {
        if (err) {
            console.log(err);
        } else {
        }
    });
};

