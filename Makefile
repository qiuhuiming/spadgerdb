test: test_bloom_filter test_dbformat test_skiplist

test_bloom_filter:
	python3 -m unittest test.bloom_filter_test

test_skiplist:
	python3 -m unittest test.skiplist_test

test_dbformat:
	python3 -m unittest test.dbformat_test