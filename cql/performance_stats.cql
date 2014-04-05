--OrderPreservingPartitioner
CREATE TABLE IF NOT EXISTS page_by_time_score (
	commit blob,
	pageID blob,
	time timestamp,
	PRIMARY KEY(commit, pageID)
) ;


CREATE TABLE IF NOT EXISTS page_by_size_score (
	commit blob,
	pageID blob,
	size int,
	PRIMARY KEY(commit, pageID)
);


