
http://www.microsoft.com/zh-cn/download/details.aspx?id=11774  download sqljdbc4.jar

http://www.cnblogs.com/timelyxyz/archive/2012/10/09/2716759.html -- install in your local


On Intel I5-4120M cpu with 8G memeory, win64 bit syste, normal SATA disk.
Loading all data used (use mongo java driver's batch insert)
02:06:52.199 [main] INFO  spcache.Loader -  all inserted using time :11307180 ms
Total disk used 83G. Average object size is 1008B per mongo collection stats().

Query performance not touched

Also mongo batch update not supported, means we might not able to use batch to reduce the network overhead. MongoDB has a JIRA SERVER-2172 for this feature reqeust, but not supported right now. --Update: mongo 2.6 provide bulk operations, see

http://docs.mongodb.org/manual/core/bulk-write-operations/
http://docs.mongodb.org/manual/reference/method/db.collection.initializeUnorderedBulkOp/#db.collection.initializeUnorderedBulkOp
http://docs.mongodb.org/manual/reference/method/js-bulk/
