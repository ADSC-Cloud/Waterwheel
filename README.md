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
    
3. How to use bloom filter?
    1. We can create bloom filters for different classes (including primitive data types wrapper classes and user-defined
    classes) through a BloomFilterHandler object.
        ```
        BloomFilterHandler bfHandler = new BloomFilterHandler("localFileName");
        BloomFilter<T> bf = bfHandler.createTBloomFilter();
        ```
       
    2. We can put any T object into this bloom filter.
        ```
        T t = new T();
        bfHandler.put(t);
        ```
        or
        ```
        T t = new T();
        bf.put(t);
        ```
    3. For persistent storage of bloom filters, we can store and load it to/from HDFS. Remember, when we reconstruct
    bloom filters from HDFS, we need to provide our class's funnel object so that the system knows how to deserialize
    the data.
        ```
        // write to HDFS
        bfHandler.store()
        
        // read from HDFS
        BloomFilter<T> bf = bfHandler.load(DataFunnel.getTFunnel); 
        ```
    4. We can get hint on whether an object is contained in the bloom filter.
        ```
        T t = new T();
        boolean b = bfHandler.mightContain(t);
        ```
        or
        ```
        T t = new T();
        boolean b = bf.mightContain(t);
        ```
    5. We can extend bloom filter to any classes.
    
          So far I have implemented bloom filters for all Java primitive types (boolean, char, short, int, long, float
          double) and String. If necessary, customized classes (such as Car) can be also implemented by simply implementing
          it Funnel by implementing Funnel<T> interface.