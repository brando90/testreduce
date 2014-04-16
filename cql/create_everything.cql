CREATE KEYSPACE IF NOT EXISTS "testreducedb"
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};

use testreducedb;

-- Results of a particular test at a time.
CREATE TABLE IF NOT EXISTS results (
    test blob,
    tid timeuuid,
    result text,
    PRIMARY KEY(test, tid)
);

-- Map commits to score and test.
CREATE TABLE IF NOT EXISTS test_by_score (
    commit blob,
    delta int,
    score int,
    test blob,r
    PRIMARY KEY(commit, delta, test)
);

-- List of tests
CREATE TABLE IF NOT EXISTS tests (
    test blob,
    PRIMARY KEY (test)
);

-- Store all of our commits with their associated timestamps.
CREATE TABLE IF NOT EXISTS commits (
    hash blob,
    tid timeuuid,
    keyframe boolean,
    PRIMARY KEY(hash)
);

-- Store revision/commit summaries so we don't have to calculate everything every time
-- should i put this in the commits table instead?
CREATE TABLE IF NOT EXISTS revision_summary (
    revision blob,
    errors int,
    skips int,
    fails int,
    numtests int,
    PRIMARY KEY(revision)
);

----
----
---- Tables for pages largest times
CREATE TABLE IF NOT EXISTS largest_time_total (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);


CREATE TABLE IF NOT EXISTS largest_time_wt2html (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);


CREATE TABLE IF NOT EXISTS largest_time_html2wt (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);

-- Tables for pages largest size
CREATE TABLE IF NOT EXISTS largest_size_htmlraw (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);


CREATE TABLE IF NOT EXISTS largest_size_htmlgzip (
	hash blob,
	tid timeuuid,	
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);


CREATE TABLE IF NOT EXISTS largest_size_wtraw (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);

CREATE TABLE IF NOT EXISTS largest_size_wtgzip (
	hash blob,
	tid timeuuid,
	sorted_list_top_largest text,
	sorted_list_corresponding_test text,
	PRIMARY KEY(hash)
);
