# Independent Sets Condition Searching Tools for LPMLN
# Dependencies
+ Python 3.6
+ MatterMost 4 (optional)

# Configuration
+ rename `lpmln-isets-sample.ini` to `lpmln-isets.ini` 
    + set `io_data_dir` **Important!**
    + set accounts of master and worker servers **Important!**
        + create the same accounts on all servers
        + set `ssh_user_name` and `ssh_password` 
+ rename `logging-sample.ini` to `logging.ini` 
+ rename `isets-tasks-sample.json` to `isets-tasks.json` 


# Searching Algorithms
+ Basic Searching Algorithm `lpmln.search.BasicSearchAlg.py` 
+ Optimized Searching Algorithm `lpmln.search.distributed.final`
    + start `lpmln.search.distributed.final.init_task_master` on master server
    + start `lpmln.search.distributed.final.init_task_worker` on worker servers
    
# SE-Conditions Simplification
+ Simplification Algorithm `lpmln.iset.IConditionCliquesUtils.group_and_simplify_se_condtions`