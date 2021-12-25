test: test_bloom_filter test_dbformat test_skiplist test_memtable test_db test_log test_write_batch

test_bloom_filter:
	python3 -m unittest test.bloom_filter_test

test_skiplist:
	python3 -m unittest test.skiplist_test

test_dbformat:
	python3 -m unittest test.dbformat_test

test_memtable:
	python3 -m unittest test.memtable_test

test_db:
	python3 -m unittest test.db_test

test_log:
	python3 -m unittest test.log_test

test_write_batch:
	python3 -m unittest test.write_batch_test

test_version_edit:
	python3 -m unittest test.version_edit_test