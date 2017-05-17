1. How to use use HDFS in Storm?
   1. add storm-hdfs and hadoop-client dependencies in POM;
   2. add HDFSBoltConfig.java and HDFSHandler.java in metadata package;
   3. set a bolt that is used to receive meta log data and then store them in HDFS.
   Suppose normal bolts (bolt1 and bolt2) are intending to emit meta log records to HDFS, then
   we just need to add one line code to the main class:
       ```
       // original code, no changes are required
       TopologyBuilder builder = new TopologyBuilder();
       builder.setSpout("spout", new Spout(), parallelism_hint);
       ...
       builder.setBolt("bolt1", new Bolt1(), parallelism_hint).shuffleGrouping("spout");
       builder.setBolt("bolt2", new Bolt2(), parallelism_hint).shuffleGrouping("spout");
       ...
              
       // new code, and should be added accordingly
       builder.setBolt("hdfs-bolt", HDFSHandler.getHdfsWritterBolt(), parallelism_hint)
                    .shuffleGrouping("bolt1", HDFSHandler.getFromStream())
                    .shuffleGrouping("bolt2", HDFSHandler.getFromStream());
       ```
         
       Then in the bolt1 and bolt2, we should add a new named stream and a new method call to emit
       meta log records to that stream:
       ```
       @Override
       public void execute(Tuple tuple) {
       
           // produce and emit FileMetaData records to HDFS bolt
           FileMetaData fileMetaData = ...;
           outputCollector.emit(HDFSHandler.emitToStream(), tuple, HDFSHandler.setValues(fileMetaData));
           
           ...
       }
    
       @Override
       public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
           outputFieldsDeclarer.declareStream(HDFSHandler.emitToStream(), HDFSHandler.setFields());
           
           ...
       }
       ```
2. How to reconstruct a RTree in cases when system crashed?

    Use static method reconstructRTree() of HDFSHandler class to return a RTree. This method will
    read recursively all meta log files located under logging/ folder and reconstruct a new RTree.
    Furthermore, this RTree can be visualized by calling visualizeRTree(targetRTree) method.
    Thus the target RTree structure will be output as a "RTree.png" image located in target/ folder.