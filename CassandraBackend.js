var util = require('util'),
  events = require('events'),
  cass = require('node-cassandra-cql'),
  consistencies = cass.types.consistencies,
  uuid = require('node-uuid'),
  PriorityQueue = require('priorityqueuejs'),
  async = require('async'),
  insertFunc = require('insert_function.js');

function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
}

// Constructor
function CassandraBackend(name, config, callback) {
    var self = this;

    //indicates how many largest values we are going to have
    //at the moment hard coded.
    //should be included in the settings file
    this.k = 100;
	
    this.name = name;
    this.config = config;
    // convert consistencies from string to the numeric constants
    var confConsistencies = config.backend.options.consistencies;
    this.consistencies = {
        read: consistencies[confConsistencies.read],
        write: consistencies[confConsistencies.write]
    };
    self.client = new cass.Client(config.backend.options);

    var reconnectCB = function(err) {
        if (err) {
            // keep trying each 500ms
            console.error('pool connection error, scheduling retry!');
            setTimeout(self.client.connect.bind(self.client, reconnectCB), 500);
        }
    };
    this.client.on('connection', reconnectCB);
    this.client.connect();

    var numFailures = config.numFailures;

    self.commits = [];

    self.testQueue = new PriorityQueue( function(a, b) { return a.score - b.score; } );
    self.runningQueue = [];
    self.testsList = {};

    // Load all the tests from Cassandra - do this when we see a new commit hash
    var statusOfSetup = function(err){
		if (err) {
            console.log( 'failure in setup due to error: ', err );
        }else if(this.emptyCommits || this.emptyTests || this.emptyTestByScore ){
			//printing which tables are empty. 
			//No news are good news (i.e. if it doesn't say its empty, its not empty)
			
			console.log("Printing information on empty tables:");
			if(this.emptyCommits){
				console.log("Empty commits table");
			}
			if(this.emptyTests){
				console.log("Empty Tests table");
			}
			if(this.emptyTestByScore){
				console.log("Empty test_by_score");	
			}
		
		}
        console.log( 'in memory queue setup complete' );
	};
	async.waterfall([getCommits.bind( this ), getTests.bind( this ), initTestPQ.bind( this )], statusOfSetup.bind( this ));

	// initLargestLists.bind( this )();

    callback();
}

// cb is getTests
function getCommits(cb) {
    var queryCB = function (err, results) {
        //console.log(results);
		//process.exit(0);
		console.log("TRUE OR FALSE?", this.emptyCommits || this.emptyTests || this.emptyTestByScore);
		if (err) {
			console.log("getCommits threw an Error!");
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            //console.log( 'no seen commits, error in database' );
            cb("no seen commits, error in database");
        } else {
			console.log("results.row.length: ", results.rows.length);
            for (var i = 0; i < results.rows.length; i++) {
                var commit = results.rows[i];
                // commits are currently saved as blobs, we shouldn't call toString on them...
                // commit[0].toString()
				//console.log("commit: ", commit)
                this.commits.push( { hash: commit[0], timestamp: commit[1], isKeyframe: commit[2] } );
            }
            this.commits.sort( function(a, b) { return b > a } );
            cb(null);
        }
    };

    // get commits to tids
    var cql = 'select hash, dateOf(tid), keyframe from commits';
    this.client.execute(cql, [], this.consistencies.write, queryCB.bind(this));
}

// cb is initTestPQ
function getTests(cb) {
    var queryCB = function (err, results) {
        //console.log(results.rows.length);
		//console.log("!results", !results);
		//console.log("!results.rows", !results.rows);
		//process.exit(0);
		//console.log("TRUE OR FALSE?", this.emptyCommits || this.emptyTests || this.emptyTestByScore);	
		if (err) {
			console.log("getTests threw an Error!");
            cb(err);
        } else if (!results || !results.rows || results.rows.length == 0) {
            console.log( 'no seen commits, error in database' );
            this.emptyTests = true;
			cb(null, 0, 0);
        } else {
            // I'm not sure we need to have this, but it exists for now till we decide not to have it.
            for (var i = 0; i < results.rows.length; i++) {
                this.testsList[results.rows[i]] = true;
            }
            cb(null, 0, results.rows.length);
        }
    };

    // get tests
    var cql = 'select test from tests;';

    // And finish it off
    this.client.execute(cql, [], this.consistencies.write, queryCB.bind( this ));
}

function initTestPQ(commitIndex, numTestsLeft, cb) {
    var queryCB = function (err, results) {
		if (err) {
            console.log('initTestPQ threw an Error');
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            this.emptyTestByScore = true;
			cb(null);
        } else {
            for (var i = 0; i < results.rows.length; i++) {
                var result = results.rows[i];
                this.testQueue.enq( { test: result[0], score: result[1], commit: result[2].toString(), failCount: 0 } );
            }

            if (numTestsLeft == 0 || this.commits[commitIndex].isSnapshot) {
                cb(null);
            }

            if (numTestsLeft - results.rows.length > 0) {
                var redo = initTestPQ.bind( this );
                redo( commitIndex + 1, numTestsLeft - results.rows.length, cb);
            }
            cb(null);
        }
    };
    var lastCommit = this.commits[commitIndex].hash;
        lastHash = lastCommit && lastCommit.hash || '';
    if (!lastHash) {
      cb(null);
    }
    var cql = 'select test, score, commit from test_by_score where commit = ?';
    

    this.client.execute(cql, [lastCommit], this.consistencies.write, queryCB.bind( this ));
}

//function initLargestLists(){
//	var cqlLargestTimeTotal = "SELECT * FROM ";
//
//}

/**
 * Get the number of regressions based on the previous commit
 *
 * @param commit1 object {
 *  hash: <git hash string>
 *  timestamp: <git commit timestamp date object>
 * }
 * @param cb function (err, num) - num is the number of regressions for the last commit
 */
CassandraBackend.prototype.getNumRegressions = function (commit, cb) {
  var fakeNum = 3;
  cb(null, fakeNum);
};

CassandraBackend.prototype.removePassedTest = function(testName) {
    for (var i = 0; i < this.runningQueue.length; i++) {
        var job = this.runningQueue[i];
        if (job.test === testName) {
            this.runningQueue.splice(i, 1);
            break;
        }
    }
};

CassandraBackend.prototype.getTestToRetry = function() {
    for (var i = 0, len = this.runningQueue.length, currTime = new Date(); i < len; i++) {
        var job = this.runningQueue[this.runningQueue.length - 1];
        if ((currTime.getMinutes() - job.startTime.getMinutes()) > 10) {
            this.runningQueue.pop();
            if (job.test.failCount < this.numFailures) {
                job.test.failCount ++;
                return job;
            } else {
                // write failed test into cassandra data store
            }
        } else {
            break;
        }
    }
    return undefined;
};

CassandraBackend.prototype.updateCommits = function(lastCommitTimestamp, commit, date) {
    console.log("lastCommitTimestamp < date: ", lastCommitTimestamp < date);
	if (lastCommitTimestamp < date) {
        this.commits.unshift( { hash: commit, timestamp: date, isKeyframe: false } );
        cql = 'insert into commits (hash, tid, keyframe) values (?, ?, ?);';
        args = [new Buffer(commit), tidFromDate(date), false];
        this.client.execute(cql, args, this.consistencies.write, function(err, result) {
            if (err) {
                console.log(err);
            }
        });
    }
}

/**
 * Get the next test to run
 *
 * @param commit object {
 * hash: <git hash string>
 * timestamp: <git commit timestamp date object>
 * }
 * @param cb function (err, test) with test being an object that serializes to
 * JSON, for example [ 'enwiki', 'some title', 12345 ]
 */
CassandraBackend.prototype.getTest = function (clientCommit, clientDate, cb) {
    var retry = this.getTestToRetry(),
        lastCommitTimestamp = this.commits[0].timestamp,
        retVal = { error: { code: 'ResourceNotFoundError', messsage: 'No tests to run for this commit'} };

    this.updateCommits(lastCommitTimestamp, clientCommit, clientDate);
	console.log();
	console.log("lastCommitTimestamp > clientDate: " , lastCommitTimestamp > clientDate);
	console.log("clientCommit: ", clientCommit);
    console.log("::::::> lastCommitTimestamp: ", lastCommitTimestamp);
	console.log("::::::::> clientDate: ", clientDate);
	console.log("retry: ", retry);
	console.log("this.testQueue.size(): ", this.testQueue.size());
	if (lastCommitTimestamp > clientDate) {
		retVal = { error: { code: 'BadCommitError', message: 'Commit too old' } };
    } else if (retry) {
		console.log("::::::> retry statment (in getTest)");
        retVal = { test: retry };
    } else if (this.testQueue.size()) {
		console.log("::::::::::> this.testQueue.size() statment");
        var test = this.testQueue.deq();
        //ID for identifying test, containing title, prefix and oldID.
        this.runningQueue.unshift({test: test, startTime: new Date()});
        retVal = { test : test.test };
    }else{
		console.log("went into NONE of the if clauses");	
	}

    cb(retVal);
};

/**
 * Get results ordered by score
 *
 * @param cb- (err, result), result is defined below
 *
    


 */
CassandraBackend.prototype.getStatistics = function(commit, cb) {

    /**
     * @param result
     *  Required results:
        numtests-  
        noerrors- numtests - ()
        noskips- ()
        nofails
        latestcommit
        crashes
        beforelatestcommit
        numfixes
        numreg
     *

    how to compute a commit summary just by test_by_scores
    1) use a commit and search through all test_by_scores
    2) compute the amount of errors, skips, and fails 
    num tests = num quered
        - Go through each, and for every tests
          If(score == 0) then noerrors++ ; nofails++; noskips++;
          else IF(score > 1000000) -> do nothing
          else If(score > 1000) (it's a fail = noerrors++) 
          else If(score > 0 ) (it's a skip = noerrors++; no fails++) 
    3) We have latest commit, num tests and For now, 
    just mock the data for numreg, numfixes, and crashes and latest commit


    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}'), 28487);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Salfoeld\""}'), 192);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Aghnadarragh\""}'), 10739);

     */

 
    var args = [], 
    results = {};

    var cql = "select score from test_by_score where commit = ?"
    args = args.concat([commit]);
    this.client.execute(cql, args, this.consistencies.write, function(err, results) {
        if (err) {
            console.log("err: " + err);
            cb(err);
        } else if (!results || !results.rows) {
            console.log( 'no seen commits, error in database' );
            cb(null);
        } else {
            //console.log("hooray we have data!: " + JSON.stringify(results, null,'\t'));
            var noerrors = 0, nofails = 0, noskips = 0;
            var numtests = results.rows.length;
            async.each(results.rows, function(item, callback) {
                //console.log("item: " + JSON.stringify(item, null,'\t'));
                var data = item[0];
                if(data < 1000000) {
                  if(data == 0) {
                    noerrors++;
                    noskips++;
                    nofails++;
                  } else if(data > 1000) {
                    noerrors++;
                  } else if(data > 0) {
                    noerrors++;
                    nofails++;
                  }
                }
                callback();
            }, function(err) {
                results = {
                    numtests: numtests,
                    noerrors: noerrors,
                    noskips: noskips,
                    nofails: nofails,
                    latestcommit: commit.toString()
                };
                console.log("result: " + JSON.stringify(results, null,'\t'));
                cb(null, results);

            })
        }
    })
    //var results = {};
    
}

/**
 * Add a result to storage
 *
 * @param test string representing what test we're running
 * @param commit object {
 *    hash: <git hash string>
 *    timestamp: <git commit timestamp date object>
 * @param cb callback (err) err or null
 */
CassandraBackend.prototype.addResult = function(test, commit, result, cb) {
    console.log("CALLING addResult");
	this.removePassedTest(test);
    cql = 'insert into results (test, tid, result) values (?, ?, ?);';
    tid = tidFromDate(new Date())
    args = [test, tid, result];
    this.client.execute(cql, args, this.consistencies.write, function(err, result) {
        if (err) {
            console.log(err);
        } else {
        }
    });	
    this.addResultToLargestTable(commit, tid, result, test);
}


/**
* Add a result to the corresponding largest size/time table.
* @param commit object {
*    hash: <git hash string>
*    timestamp: <git commit timestamp date object>
* @param tid 
* @param result the result string in XML form
* @test that generated this result (TODO CHECK THIS)
**/
CassandraBackend.prototype.addResultToLargestTable= function(commit, tid, result, test){
    var result_parsed_obj = this.parsePerfStats(result);
    var type_of_cql = result_parsed_obj[type]
    var types = type_of_cql.split(":");
    var type = types[0]; //size or time
    var type_name = type[1]; 
    var new_value = result_parsed_obj[value]
    var tableName = "largest_"+type+"_"+type_name;

    var select_cql = "SELECT (sorted_list_top_largest) FROM "+tableName+" WHERE commit = (?)";
    var update_cql = "INSERT INTO "+tableName+" (commit, timeuuid, sorted_list_top_largest) VALUES (?, ?, ?)";
    this.updateLargestResultsTable(select_cql, update_cql, commit, tid, new_value);
}

CassandraBackend.prototype.updateLargestResultsTable = function(select_cql, update_cql, commit, tid, new_value){
    var queryCB = function(err, results){
        //get the sorted list from the DB and then try to insert new_value if appropriate
        if(err){
            console.log(err);
        } else if (results.rows.length > 1 ) {
            console.log("There should never be two rows with the same commit.");
        } else{
            var sorted_list;
            var sorted_list_json_str;
            if (!results || !results.rows || results.rows.length === 0) {
                //if this is the first time we are adding results, then just add it!
                sorted_list = [new_value];
                sorted_list_json_str =  JSON.stringify(sorted_list);
                this.client.execute(update_cql, [commit, tid, sorted_list_json_str], this.consistencies.write, cb);
            } 
            var result = results.rows[0];
            sorted_list = JSON.parse(result[3]);
            if(sorted_list.length < this.k){
                sorted_list = insertFunc.insert(sorted_list, new_value);
                sorted_list_json_str = JSON.stringify(sorted_list);
                this.client.execute(update_cql, [commit, tid, sorted_list_json_str], this.consistencies.write, cb);
            }else{
                var smallest_element = sorted_list[sorted_list.length]
                if(smallest_element < new_value){
                    sorted_list = insertFunc.insert(sorted_list, new_value);
                    sorted_list =  sorted_list.slice(1, sorted_list.length);
                    sorted_list_json_str = JSON.stringify(sorted_list);
                    this.client.execute(update_cql, [commit, tid, sorted_list_json_str], this.consistencies.write, cb);
                }
            }
        }

    }
    //get the largest values so far before updating them
    this.client.execute(select_cql, [commit], this.consistencies.write, queryCB.bind(this)); 
}

CassandraBackend.prototype.parsePerfStats = function( text) {
    var regexp = /<perfstat[\s]+type="([\w\:]+)"[\s]*>([\d]+)/g;
    var perfstats = [];
    for ( var match = regexp.exec( text ); match !== null; match = regexp.exec( text ) ) {
        perfstats.push( { type: match[ 1 ], value: match[ 2 ] } );
    }
    return perfstats;
};

var statsScore = function(skipCount, failCount, errorCount) {
    // treat <errors,fails,skips> as digits in a base 1000 system
    // and use the number as a score which can help sort in topfails.
    return errorCount*1000000+failCount*1000+skipCount;
};

/**
 * Get results ordered by score
 *
 * @param offset (for pagination)
 * @param limit  (for pagination)
 * @param cb
 *
 */
CassandraBackend.prototype.getFails = function(offset, limit, cb) {

    /**
     * cb
     *
     * @param results array [
     *    object {
     *      commit: <commit hash>,
     *      prefix: <prefix>,
     *      title:  <title>
     *      status: <status> // 'perfect', 'skip', 'fail', or null
     *      skips:  <skip count>,
     *      fails:  <fails count>,
     *      errors: <errors count>
     *      }
     * ]
     */
    cb([]);
}


// Node.js module exports. This defines what
// require('./CassandraBackend.js'); evaluates to.
module.exports = CassandraBackend;
