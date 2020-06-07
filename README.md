# Extended Independent Sets Condition Searching Tools for LPMLN
# Dependencies
+ Python 3.6
+ MatterMost 4 (optional)

# Configuration
+ rename `lpmln-isets-sample.ini` to `lpmln-isets.ini` 
    + set `io_data_dir` **Important!**
+ rename `logging-sample.ini` to `logging.ini` 
+ rename `isets-tasks-sample.json` to `isets-tasks.json`

# Initialization
+ run `lpmln.search.misc.SearchingInit.init()`

# Searching Algorithms
+ Basic Searching Algorithm `lpmln.search.BasicSearchAlg.py` 
+ Optimized Searching Algorithm `lpmln.search.DistributedISCSearchFile.py`
    + start `init_kmn_isc_task_master_from_config` on master server
    + start `init_kmn_isc_task_worker` on worker servers