var util = require('util'),
  events = require('events'),
  cass = require('node-cassandra-cql'),
  consistencies = cass.types.consistencies,
  uuid = require('node-uuid'),
  PriorityQueue = require('priorityqueuejs'),
  async = require('async'),
  insertFunc = require('./insert_function.js');

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

    this.name = name;
    this.config = config;

    //indicates how many largest values we are going to have
    //should be included in the settings file
    this.num_largest_values_tracking = config.backend.num_largest_values_tracking;
    //this.num_largest_values_tracking = 100;

    // convert consistencies from string to the numeric constants
    var confConsistencies = config.backend.options.consistencies;
    this.consistencies = {
        read: consistencies[confConsistencies.read],
        write: consistencies[confConsistencies.write]
    };
    self.client = new cass.Client(config.backend.options);

    var reconnectCB = function (err) {
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

    self.testQueue = new PriorityQueue(function (a, b) {
        return a.score - b.score;
    });
    self.runningQueue = [];
    self.testsList = {};
    self.latestRevision = {};
    self.topFailsArray = [];

    // Load all the tests from Cassandra - do this when we see a new commit hash
    var statusOfSetup = function(err){
		if (err) {
            console.log( 'failure in setup due to error: ', err );
        }
        console.log( 'in memory queue setup complete' );
	};
	async.waterfall([getCommits.bind( this ), getTests.bind( this ), initTestPQ.bind( this )], statusOfSetup.bind( this ));

    callback();
}

// cb is getTests

//I did :
// insert into commits (hash, tid, keyframe) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 5b89fc70-ba95-11e3-a5e2-0800200c9a66, true);
// insert into commits (hash, tid, keyframe) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), d0602570-b52b-11e3-a5e2-0800200c9a66, true);
function getCommits(cb) {
    var queryCB = function (err, results) {
		if (err) {
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            cb("no seen commits, error in database");
        } else {
            for (var i = 0; i < results.rows.length; i++) {
                var commit = results.rows[i];
                // commits are currently saved as blobs, we shouldn't call toString on them...
                // commit[0].toString()
                this.commits.push({
                    hash: commit[0],
                    timestamp: commit[1],
                    isKeyframe: commit[2]
                });
            }
            this.commits.sort(function (a, b) {
                return b > a
            });
            //console.log("commits: " + JSON.stringify(this.commits, null,'\t'));
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
		if (err) {
            cb(err);
        } else if (!results || !results.rows || results.rows.length == 0) {
            console.log( 'no seen commits, error in database' );
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
    this.client.execute(cql, [], this.consistencies.write, queryCB.bind(this));
}

//note to the person doing inittestpq, this function will call cb(null) twice
//the line after checking if we have no tests left 
function initTestPQ(commitIndex, numTestsLeft, cb) {
    var queryCB = function (err, results) {
		if (err) {
            //console.log('initTestPQ threw an Error');
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            this.emptyTestByScore = true;
			cb(null);
        } else {
            for (var i = 0; i < results.rows.length; i++) {
                var result = results.rows[i];
                this.testQueue.enq({
                    test: result[0],
                    score: result[1],
                    commit: result[2].toString(),
                    failCount: 0
                });
            }
            if (numTestsLeft == 0 || this.commits[commitIndex].isSnapshot) {
                return cb(null);
            }

            if (numTestsLeft - results.rows.length > 0) {
                var redo = initTestPQ.bind(this);
                return redo(commitIndex + 1, numTestsLeft - results.rows.length, cb);
            }
            cb(null);
        }
    };
    var lastCommit = this.commits[commitIndex].hash;
    lastHash = lastCommit && lastCommit.hash || '';
    this.latestRevision.commit = lastCommit;
    //console.log("lastcommit: " + lastCommit + " lasthash: " + lastHash );
    if (!lastCommit) {
        cb(null);
    }
    var cql = 'select test, score, commit from test_by_score where commit = ?';


    this.client.execute(cql, [lastCommit], this.consistencies.write, queryCB.bind(this));
}

function initTopFails(cb) {
    var queryCB = function (err, results) {
        if (err) {
            console.log('in error init top fails');
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            console.log("no results found")
            cb(null);
        } else {
            for (var i = 0; i < results.rows.length; i++) {
                var result = results.rows[i];
                var index = findWithAttr(this.topFailsArray, "test", result[0]);
                if (index === -1 || this.topFailsArray === undefined ) {
                    this.topFailsArray.push({ test: result[0], score: result[1], commit: result[2].toString()});
                } else if(this.topFailsArray[index].score <= result[1]) {
                    this.topFailsArray[index] ={ test: result[0], score: result[1], commit: result[2].toString()};
                }
            }

            this.commitFails++;
            if (this.commitFails < this.commits.length) {
                var redo = initTopFails.bind( this );
                redo(cb);
            } else { 
              cb(null, this.topFailsArray);
            }
        }
    };
    this.commitFails = (this.commitFails !== undefined) ? this.commitFails :  0;
    //console.log("this.commits[0]: " + this.commitFails + "is "  + JSON.stringify(this.commits[0]));
    
    if(!this.commits[this.commitFails]) {
        //console.log("finished!: " + this.commitFails + "stuff: " + JSON.stringify(this.topFailsArray, null,'\t'));
        console.log("ran out of commits??")
        return cb(null);
    }
    var lastCommit = this.commits[this.commitFails].hash;
        lastHash = lastCommit && lastCommit.hash || '';
    //console.log("commit table: " + JSON.stringify(this.commits, null,'\t'));
    if (!lastCommit) {
      var error = "no last commit";
      //console.log("no last commit");
      cb(error);
    }
    var cql = 'select test, score, commit from test_by_score where commit = ?'; //TODO this doesnt have a ; at the end of cql. issue?
    

    this.client.execute(cql, [lastCommit], this.consistencies.write, queryCB.bind( this ));
}

function findWithAttr(array, attr, value) {
    for(var i = 0; i < array.length; i++) {
        //console.log("finding: " + typeof(array[i].test) + " comparing: " + typeof(value));
        if(array[i][attr].toString() === value.toString()) {
            //console.log("found!")
            return i;
        }
    }
    return -1;
}

CassandraBackend.prototype.getTFArray = function(cb) {
    if(!this.topFailsArray || this.topFailsArray.length === 0) {
        return cb("empty or nonexistent array");
    } else {
    
        return cb(null, this.topFailsArray);
    }
}

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

CassandraBackend.prototype.removePassedTest = function (testName) {
    for (var i = 0; i < this.runningQueue.length; i++) {
        var job = this.runningQueue[i];
        if (job.test === testName) {
            this.runningQueue.splice(i, 1);
            break;
        }
    }
};

CassandraBackend.prototype.getTestToRetry = function () {
    for (var i = 0, len = this.runningQueue.length, currTime = new Date(); i < len; i++) {
        var job = this.runningQueue[this.runningQueue.length - 1];
        if ((currTime.getMinutes() - job.startTime.getMinutes()) > 10) {
            this.runningQueue.pop();
            if (job.test.failCount < this.numFailures) {
                job.test.failCount++;
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

CassandraBackend.prototype.updateCommits = function (lastCommitTimestamp, commit, date) {
    if (lastCommitTimestamp < date) {
        this.commits.unshift({
            hash: commit,
            timestamp: date,
            isKeyframe: false
        });
        cql = 'insert into commits (hash, tid, keyframe) values (?, ?, ?);';
        args = [new Buffer(commit), tidFromDate(date), false];
        this.client.execute(cql, args, this.consistencies.write, function (err, result) {
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
        retVal = {
            error: {
                code: 'ResourceNotFoundError',
                messsage: 'No tests to run for this commit'
            }
        };

    this.updateCommits(lastCommitTimestamp, clientCommit, clientDate);
    if (lastCommitTimestamp > clientDate) {
        retVal = {
            error: {
                code: 'BadCommitError',
                message: 'Commit too old'
            }
        };
    } else if (retry) {
        retVal = {
            test: retry
        };
    } else if (this.testQueue.size()) {
        var test = this.testQueue.deq();
        //ID for identifying test, containing title, prefix and oldID.
        this.runningQueue.unshift({
            test: test,
            startTime: new Date()
        });
        retVal = {
            test: test.test
        };
    }
    cb(retVal);
};


/**
Computes the number of regression and fixes based on deltas
**/
CassandraBackend.prototype.getNumRegFix = function(cb) {
  var args = [];
  var cql = "select delta from test_by_score where commit = ?";
  args = args.concat([this.latestRevision.commit]);

  this.client.execute(cql, args, this.consistencies.write,function(err, results) {
    if (err) {
        console.log("err: " + err);
        cb(err);
    } else if (!results || !results.rows) {
        console.log('no seen commits, error in database');
        cb(null);
    } else {
      var data = results.rows;
      var res = {
        reg: 0,
        fix: 0
      }
      //console.log("data: " + JSON.stringify(data,null,'\t'));
      for(var y in data) {
        if(data[y][0] > 0) {
            res.reg++;
        } else if(data[y][0] < 0) {
            res.fix++;
        }
      }
      cb(null, res);
    }
  })
}

/**
 * Get results ordered by score
 *
 * @param cb- (err, result), result is defined below
 *
    


 */
CassandraBackend.prototype.getStatistics = function (commit, cb) {

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
    
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}'), 10500);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Salfoeld\""}'), 1050);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Aghnadarragh\""}'), 100);
     */
    var args = [],
        results = {};


    //if it's not the latest revision AND latestRevision isn't empty, 
    //then we can just look it up in the revision summary table

    //else if it's the latest revision, we have to dynamically compute it and then insert
    var cql = "select score from test_by_score where commit = ?"
    args = args.concat([commit]);

    var getRegFixes = this.getNumRegFix.bind(this);
    this.client.execute(cql, args, this.consistencies.write, function (err, results) {
        if (err) {
            console.log("err: " + err);
            cb(err);
        } else if (!results || !results.rows) {
            console.log('no seen commits, error in database');
            cb(null);
        } else {
            //console.log("hooray we have data!: " + JSON.stringify(results, null,'\t'));
            var numtests = results.rows.length;
            getRegFixes(function(err, data) {
              extractESF(results.rows, function (err, ESFdata) {
                var results = {
                    numtests: numtests,
                    noerrors: ESFdata.noerrors,
                    noskips: ESFdata.noskips,
                    nofails: ESFdata.nofails,
                    latestcommit: commit.toString(),
                    numReg: data.reg,
                    numFixes: data.fix
                }
                cb(null, results);
              });
            });

        }
    })
    //var results = {};

}

var extractESF = function (rows, cb) {
    var noerrors = 0,
        nofails = 0,
        noskips = 0;
    async.each(rows, function (item, callback) {
        //console.log("item: " + JSON.stringify(item, null,'\t'));
        var data = item[0];
        if (data < 1000000) {
            if (data == 0) {
                noerrors++;
                noskips++;
                nofails++;
            } else if (data > 1000) {
                noerrors++;
            } else if (data > 0) {
                noerrors++;
                nofails++;
            }
        }
        callback();
    }, function (err) {
        results = {
            noerrors: noerrors,
            noskips: noskips,
            nofails: nofails,
        };
        //console.log("result: " + JSON.stringify(results, null,'\t'));
        cb(null, results);

    })
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
CassandraBackend.prototype.addResult = function(test, commit, result) {
    this.latestRevision.commit = commit;
	this.removePassedTest(test);
    cql = 'insert into results (test, tid, result) values (?, ?, ?);';
    tid = tidFromDate(new Date())
    args = [test, tid, result];
    this.client.execute(cql, args, this.consistencies.write, function(err, result) {
        if (err) {
            console.log(err);
        } else {}
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
    var result_parsed_array = this.parsePerfStats(result);
    for (var i = 0; i < result_parsed_array.length; i++){
        var current_parsed_result_obj = result_parsed_array[i];
        var types = current_parsed_result_obj["type"].split(":");
        var type = types[0]; //size or time
        var type_name = types[1]; //total, wtzhtml, ... , wtraw, wtgzip
        var new_value = current_parsed_result_obj["value"];
        var tableName = "largest_"+type+"_"+type_name;
        var select_cql = "SELECT * FROM "+tableName+" WHERE hash = ?;";
        var update_cql = "INSERT INTO "+tableName+" (hash, tid, sorted_list_top_largest, sorted_list_corresponding_test) VALUES (?, ?, ?, ?);";
        this.updateLargestResultsTable(select_cql, update_cql, commit, tid, new_value, test);
    }
}

/**
* @param select_cql: the cql query that will make sure we get the current largest k sizes/times from database. Neccesary to
*   to make sure that whatever new value are trying to add gets compared to the most recent top largest things.
* @param update_cql: the cql query that the database if the new value the checks.
* @commit: the commit we are trying to update its results
* @tid
* @new_value: the new candidate value to add to the database (its added if its larger than any of the top k current things in the databse).
* @cb: TODO: not sure if its neccesery. 
**/
CassandraBackend.prototype.updateLargestResultsTable = function(select_cql, update_cql, commit, tid, new_value, test){
    //commit = '0x'+commit;
    var commit = new Buffer(commit);
    var cb = function(err, result){ //TODO is this cb neccesery?
        if(err){
            console.log(err);
        }else{
        }
    }
    var queryCB = function(err, results){
        if(err){
            console.log("\n WENT INTO THE ERROR CASE!");
            console.log("commit: ", commit)
            console.log("results: ", results);
            console.log("select query is: ", select_cql);
            console.log("update query is: ", update_cql);
            console.log(err);
        } else if (results.rows.length > 1 ) {
            console.log("Panic: there should never be two rows or more with the same commit.");
        } else{
            var sorted_list;
            var sorted_list_json_str;
            var sorted_list_test;
            var sorted_list_corresponding_test_json_str;
            if (!results || !results.rows || results.rows.length === 0) {
                //if this is the first time we are adding results, then just add it!
                sorted_list = [new_value];
                sorted_list_json_str =  JSON.stringify(sorted_list);
                sorted_list_test = [test];
                sorted_list_corresponding_test_json_str = JSON.stringify(sorted_list_test);
                this.client.execute(update_cql, [commit, tid, sorted_list_json_str, sorted_list_corresponding_test_json_str], this.consistencies.write, cb);
            }else{
                var result = results.rows[0];
                var index_to_insert;
                sorted_list = JSON.parse(result[2]);
                sorted_list_test = JSON.parse(result[1]);
                if(sorted_list.length < this.num_largest_values_tracking){
                    //get index
                    index_to_insert = insertFunc.getIndexPositionToInsert(sorted_list, new_value);
                    //insert to sorted lists
                    sorted_list = insertFunc.insertIntoPosition(sorted_list, new_value, index_to_insert);
                    sorted_list_test = insertFunc.insertIntoPosition(sorted_list_test, test, index_to_insert);
                    //make json string
                    sorted_list_json_str = JSON.stringify(sorted_list);
                    sorted_list_corresponding_test_json_str = JSON.stringify(sorted_list_test);
                    //update database
                    this.client.execute(update_cql, [commit, tid, sorted_list_json_str, sorted_list_corresponding_test_json_str], this.consistencies.write, cb);
                }else{
                    var smallest_element = sorted_list[0];
                    if(smallest_element < new_value){
                        //get index
                        index_to_insert = insertFunc.getIndexPositionToInsert(sorted_list, new_value);
                        //insert to sorted lists
                        sorted_list = insertFunc.insertIntoPosition(sorted_list, new_value, index_to_insert);
                        sorted_list_test = insertFunc.insertIntoPosition(sorted_list_test, test, index_to_insert);
                        //chopp of the old smallest element. Makes sure list remains length <= this.num_largest_values_tracking
                        sorted_list =  sorted_list.slice(1, sorted_list.length);
                        sorted_list_test =  sorted_list.slice(1, sorted_list_test.length);
                        //make json string
                        sorted_list_json_str = JSON.stringify(sorted_list);
                        sorted_list_corresponding_test_json_str = JSON.stringify(sorted_list_test);
                        this.client.execute(update_cql, [commit, tid, sorted_list_json_str, sorted_list_corresponding_test_json_str], this.consistencies.write, cb);
                    }
                }
            } 
        }
        //cb(null); //TODO does it really need a cb?
    }
    //get the largest values so far before updating them
    this.client.execute(select_cql, [commit], this.consistencies.write, queryCB.bind(this)); 
}

/**
* @param commit = the specific commit we want to query its top k results.
* @param type_size_time = string either "size" or "time" (to query larges tin size or lowest in time).
* @param type_of_result = is the specific type of result we want to query. It should only be any of the following
* strings: (for time) total, wt2html, html2wt, or (for size) htmlraw, htmlgzip, wtraw, wtgzip.
* @param cb = callback to call on the results from the database. First argument should be an err and the second
* should expect a single array of at most k elements containing largest results so far. The thrid should also be
* a single array expecting the tests corresponding to the values from the top k largest results. So for example,
* array1[i] is the ith largest result and array2[i] is the test corresponding to that result (for the current commit).
**/
CassandraBackend.prototype.getTopLargest = function(commit, type_size_time, type_of_result, cb){
    //get the largest values so far before updating them
    var commit = new Buffer(commit);
    var queryCB = function(err, results){
        if (err){
            console.log(err);
            cb(err, null, null);
        } else if (results.rows.length > 1 ) {
            var panic_err =  "Panic: there should never be two rows or more with the same commit.";
            console.log(panic_err);
            cb(panic_err, null, null);
        }if (!results || !results.rows || results.rows.length === 0) {
            console.log("results are currently empty");
            cb(null, [], []);
        }else{
            var cdb_result = results.rows[0];
            var index_to_insert;
            var sorted_list = JSON.parse(cdb_result[2]);
            var sorted_list_test = JSON.parse(cdb_result[1]);
            cb(null, sorted_list, sorted_list_test);
        }
    }
    var tableName = "largest_"+type_size_time+"_"+type_of_result;
    var select_cql = "SELECT * FROM "+tableName+" WHERE hash = ?;";
    this.client.execute(select_cql, [commit], this.consistencies.write, queryCB.bind(this));
}

CassandraBackend.prototype.parsePerfStats = function(text) {
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
    return errorCount * 1000000 + failCount * 1000 + skipCount;
};

/**
 * Get results ordered by score
 *
 * @param offset (for pagination)
 * @param limit  (for pagination)
 * @param cb
 *
 */
CassandraBackend.prototype.getFails = function (offset, limit, cb) {

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



var regressionsHeaderData = ['Title', 'New Commit', 'Errors|Fails|Skips', 'Old Commit', 'Errors|Fails|Skips'];


//errorCount * 1000000 + failCount * 1000 + skipCount;

var regressionHelper = function(test, score1, score2) {

  var res = {
    test: test, 
    score1: score1,
    score2: score2,
    errors: 0,
    fails: 0,
    skips: 0,
    old_errors: 0,
    old_fails: 0,
    old_skips: 0
  }

  if(score1 >= 1000000) {
    res.errors = Math.floor(score1 / 1000000);
    score1 = score1 - (1000000 * res.errors);
  }
  if(score2 >= 1000000) {
    res.old_errors = Math.floor(score2 / 1000000);
    score2 = score2 - (1000000 * res.old_errors);
  }
  
  if(score1 >= 1000) {
    res.fails = Math.floor(score1 /1000);
    score1 = score1- (1000 * res.fails);
  }
  if(score2 >= 1000) {
    res.old_fails = Math.floor(score2 / 1000);
    score2 = score2- (1000 * res.old_fails);
  }

  if(score1 > 0) {
    res.skips = score1;
  }
  if(score2 > 0) {
    res.old_skips = score2;
  }

  return res;
}
/**
This method calculates all the scores data from the tests table
**/
// 33471172030bb001557200d193b402cfdf4eeaaf
// http://localhost:8001/onefailregressions/between/33471172030bb001557200d193b402cfdf4eeaaf/33471172030bb001557200d193b402cfdf4eeaaf
function calcRegressionFixes(r1, r2, cb) {
    //var data = mock.testdata;

    //if r1 is the latest revision

    //select all the test_by_scores from r1, and for each of them, select all of the scores from r2 (if exists)
    //
    console.log("this.latest: " + this.latestRevision);
    var regData = [];
    var fixData = [];

    var queries = [{
        //query: 'select test, score from test_by_score where commit = ?',
        //query: 'select * from test_by_score where commit = ?',
        query: 'select * from test_by_score',
        params: [new Buffer(r1)]
    }, {
        //query: 'select test, score from test_by_score where commit = ?',
        //query: 'select * from test_by_score where commit = ?',
        query: 'select * from test_by_score',
        params: [new Buffer(r2)]
    }];

    console.log("commit1 r1: ", r1);
    console.log("commit2 r2: ", r2);
    var firstResults = {};
    var queryCB = function (err, results) {
        if (err) {
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            console.log( "Error thrown by: calcRegressionFixes => queryCB1" );
            cb("no seen commits, error in database");
        } else {
            firstResults = results.rows;
            this.client.execute(queries[1].query, queries[1].params, this.consistencies.write, queryCB2.bind(this));
            //this.client.execute(queries[1].query, queries[1].params, this.consistencies.write, queryCB2.bind(this));
        }
    };
    var queryCB2 = function (err, results) {
        if (err) {
            cb(err);
        } else if (!results || !results.rows || results.rows.length === 0) {
            //console.log( 'no seen commits, error in database' );
            console.log("Error thrown by calcRegressionFixes => queryCB2");
            cb("no seen commits, error in database");
        } else {
            var data = results.rows;
            //console.log("results: " + JSON.stringify(results, null, '\t'));
            //go through firstResults, and for each of its tests find the corresponding one
            //in the results rows, and for each of them that are regressions, push it to the regData, else fixData
            console.log("firstresult[0][0]: " + firstResults[0][0].toString());
            cb(null, [], []); //uncomment for deployment
            return; //uncomment for deployment
            for(var y in firstResults) {
                //console.log(y);
                //console.log("result: " + firstResults[y][0].toString());
                for(var x in data) {
                    if(data[x][0].toString() === firstResults[y][0].toString()) {
                      // var ret = {
                      //     first: firstResults[y],
                      //     second: data[x]
                      // };
                      // console.log("ret: " + JSON.stringify(ret, null,'\t'));
                      var score1 = firstResults[y][1];
                      var score2 = data[x][1];
                      var test = data[x][0].toString();
                      if(score1< score2) fixData.push(regressionHelper(test, score1, score2));
                      else if (score1 > score2) regData.push(regressionHelper(test, score1, score2));
                    }
                };
                //console.log("y: " + JSON.stringify(firstResults[y],null, '\t'))
            }
            cb(null, regData, fixData);
        }
    };

    //this.client.execute(queries[0].query, queries[0].params, this.consistencies.write, queryCB.bind(this));
    this.client.execute(queries[0].query, [], this.consistencies.write, queryCB.bind(this));
    //this.client.execute(queries[0].query, [new Buffer("33333437313137323033306262303031353537323030643139336234303263666466346565616166")], this.consistencies.write, queryCB.bind(this));

    // for(var y in data) {
    //   var x = data[y];
    //   var newtest = statsScore(x.skips, x.fails, x.errors);
    //   var oldtest = statsScore(x.old_skips, x.old_fails, x.old_errors);

    //   /*if they differ then we're going to push it in either the regression or fixes*/
    //   if(newtest !== oldtest)  {
    //     /*if the new is better than the old then it's a fix, otherwise regress*/
    //     (newtest < oldtest) ?fixData.push(x) : regData.push(x);
    //   }
    // }

    // //console.log("data: " + JSON.stringify(regData, null, '\t') + "\n" + JSON.stringify(fixData,null,'\t'));
    // cb (null, regData, fixData);


}

CassandraBackend.prototype.getRegressions = function (r1, r2, prefix, page, cb) {
    var calc = calcRegressionFixes.bind(this);
    calc(r1, r2, function (err, reg, fix) {
        if (err) return cb(err);
        //return console.log("regressions: " +JSON.stringify(regressions,null,'\t'));
        async.sortBy(reg, function(item, callback) {
            callback(null, item.score2 - item.score1);
        }, function(err, regressions) {
            var mydata = {
                page: page,
                urlPrefix: prefix,
                urlSuffix: '',
                heading: "Total regressions between selected revisions: " + regressions.length,
                /*change this with mock's num regresssions*/
                headingLink: {
                    url: "/topfixes/between/" + r1 + "/" + r2,
                    name: 'topfixes'
                },
                header: regressionsHeaderData
            };

            for (var i = 0; i < regressions.length; i++) {
                regressions[i].old_commit = r2;
                regressions[i].new_commit = r1;
            }

            
            //console.log("json: " + JSON.stringify(regressions, null, '\t'));

            cb(null, regressions, mydata);
        });
    });
}

/**
 * getRegressionRows mock method returns the mock data of the fake regressions
 */
CassandraBackend.prototype.getFixes = function (r1, r2, prefix, page, cb) {
    var calc = calcRegressionFixes.bind(this);
    calc(r1, r2, function (err, reg, fix) {
        if(err) return cb(err);

        async.sortBy(fix, function(item, callback) {
            callback(null, item.score1 - item.score2);
        }, function(err, fixes) {
            var mydata = {
                page: page,
                urlPrefix: prefix,
                urlSuffix: '',
                heading: "Total fixes between selected revisions: " + fixes.length,
                /*change this with mock's num regresssions*/
                headingLink: {
                    url: '/regressions/between/' + r1 + '/' + r2,
                    name: 'regressions'
                },
                header: regressionsHeaderData
            };

            for (var i = 0; i < fixes.length; i++) {
                fixes[i].old_commit = r2;
                fixes[i].new_commit = r1;
            }
            cb(null, fixes, mydata);
        });
    });
}

//1)call the function from John's repo (calcRegressionFixes).
//2)process the three pieces of info we should partition:
// -onefailregressions
// -oneskipregressions
// -newfailsregressions
//3) feed them as return values to callback from ther server
//
// dataObj is the following: 
//
// var res = {
//   test: test, 
//   score1: score1,
//   score2: score2,
//   errors: 0,
//   fails: 0,
//   skips: 0,
//   old_errors: 0,
//   old_fails: 0,
//   old_skips: 0
// }
//     'SELECT count(*) AS numFlaggedRegressions ' +
//     'FROM pages ' +
//     'JOIN stats AS s1 ON s1.page_id = pages.id ' +
//     'JOIN stats AS s2 ON s2.page_id = pages.id ' +
//     'WHERE s1.commit_hash = ? AND s2.commit_hash = ? AND s1.score > s2.score ' +
//         'AND s2.fails = 0 AND s2.skips = 0 ' +
//         'AND s1.fails = ? AND s1.skips = ? ';
CassandraBackend.prototype.getOneDiffRegressions = function(commit1, commit2, numFails, numSkips, cb){
    //get the regression fixes and send them to be processed by ther server callback.
    var calc = calcRegressionFixes.bind(this);
    if (commit1 == "DEBUG"){
        this.getOneDiffRegressionsDEBUG(commit1, commit2, numFails, numSkips, cb);
    }
    calc(commit1, commit2, function(err, reg, fix){
        //filters the data from the regressions and sends it to the original server Call Back functionn 
        if (err){
            cb(err, null);
        }else{
            //uncomment for production, can be commented for development time.
            if (reg.length == 0){
                console.log("executed checking if reg was empty");
                cb("Error Empty: no data in regression data (reg).", null);
            }
            var collectedReg = [];
            //go through the reg, and for each piece of test information collect it, depending on which of the following condition they satisfy:
            //  1)onefailregressions
            //  2)oneskipregressions or,
            for (var i = 0; i < reg.length; i++){
                var dataObj = reg[i];
                if ( dataObj.fails == numFails && dataObj.skips == numSkips ){
                    collectedReg.push(dataObj);
                }
            }
            if(collectedReg.length == 0){
                console.log("Error Empty: onefailregressions, oneskipregressions.");
                cb("Error: no useful data in regression data (collectedReg).", null);
            }else{
                cb(null, collectedReg);
            }
        }
    });
}

/*  WHERE s1.commit_hash = ? AND s2.commit_hash = ? 
    AND s1.score > s2.score
    AND s2.fails = 0 AND s1.fails > 0
    // exclude cases introducing exactly one skip/fail to a perfect
    AND (s1.skips > 0) OR (s1.fails !> 1) OR (s2.skips > 0);
*/
CassandraBackend.prototype.getNewFailsRegressions = function(commit1, commit2, cb){
    //get the regression fixes and send them to be processed by ther server callback.
    var calc = calcRegressionFixes.bind(this);
    calc(commit1, commit2, function(err, reg, fix){
        //filters the data from the regressions and sends it to the original server Call Back functionn 
        if (err){
            cb(err, null);
        }else{
            //uncomment for production, can be commented for development time.
            if (reg.length == 0){
                console.log("executed checking if reg was empty");
                cb("Error Empty: no data in regression data (reg).", null);
            }
            var collectedReg = [];
            //go through the reg, and for each piece of test information collect it, depending on which of the following condition they satisfy:
            //  1)onefailregressions
            //  2)oneskipregressions or,
            for (var i = 0; i < reg.length; i++){
                var dataObj = reg[i];
                if ( this.isNewFail(dataObj) ){
                    collectedReg.push(dataObj);
                }
            }
            if(collectedReg.length == 0){
                console.log("Error Empty: newfailsregressions.");
                cb("Error: no useful data in regression data (collectedReg).", null);
            }else{
                cb(null, collectedReg);
            }
        }
    });
}

//AND s2.fails = 0 AND s1.fails > 0
//AND ((s1.skips > 0) OR (s1.fails != 1) OR (s2.skips > 0));
CassandraBackend.prototype.isNewFail = function(dataObj){
    var cond1 = (dataObj.old_fails == 0) && (dataObj.fails > 0);
    var cond2 = ( (dataObj.skips > 0) || (dataObj.fails != 1) || (dataObj.skips > 0) );
    return cond1 && cond2;
}

CassandraBackend.prototype.callDBdebug = function(cql, args, cb){
    this.client.execute(cql, args, this.consistencies.write, cb);
}

/*
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}'), 28487);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Salfoeld\""}'), 192);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('0b5db8b91bfdeb0a304b372dd8dda123b3fd1ab6'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Aghnadarragh\""}'), 10739);
    
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}'), 10500);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Salfoeld\""}'), 1050);
    insert into test_by_score (commit, delta, test, score) values (textAsBlob('bdb14fbe076f6b94444c660e36a400151f26fc6f'), 0, textAsBlob('{"prefix": "enwiki", "title": "\"Aghnadarragh\""}'), 100);
*/
CassandraBackend.prototype.getOneDiffRegressionsDEBUG = function(commit1, commit2, numFails, numSkips, cb){
    var reg = [];
    //commit, 0, textAsBlob('{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}'), 10500);
    var res = {
        test: '{"prefix": "enwiki", "title": "\"Slonowice_railway_station\""}', 
        score1: score1,
        score2: score2,
        errors: 0,
        fails: 1,
        skips: 0,
        old_errors: 0,
        old_fails: 0,
        old_skips: 0
    }
}

// Node.js module exports. This defines what
// require('./CassandraBackend.js'); evaluates to.
module.exports = CassandraBackend;
