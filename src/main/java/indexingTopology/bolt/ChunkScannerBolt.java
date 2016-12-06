package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Cache.*;
import indexingTopology.Config.Config;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by acelzj on 11/9/16.
 */
public class ChunkScannerBolt extends BaseRichBolt {

    OutputCollector collector;

    private int bTreeOder;

    private transient LRUCache<CacheMappingKey, CacheUnit> cacheMapping;

    private transient int numberOfCacheUnit;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        bTreeOder = 4;
        numberOfCacheUnit = 0;
        cacheMapping = new LRUCache<CacheMappingKey, CacheUnit>(Config.CACHE_SIZE);
    }

    public void execute(Tuple tuple) {

        SubQuery subQuery = (SubQuery) tuple.getValue(0);

        /*
        Long queryId = tuple.getLong(0);
        Double key = tuple.getDouble(1);
//        ArrayList<String> fileNames = (ArrayList) tuple.getValue(2);
        String fileName = (String) tuple.getValue(2);
        Long timestampLowerBound = tuple.getLong(3);
        Long timestampUpperBound = tuple.getLong(4);
        */

        Long queryId = subQuery.getQueryId();
        Double key = subQuery.getKey();
        String fileName = subQuery.getFileName();
        Long timestampLowerBound = subQuery.getStartTimestamp();
        Long timestampUpperBound = subQuery.getEndTimestamp();

        FileScanMetrics metrics = new FileScanMetrics();
        Long startTime = System.currentTimeMillis();

        Long startTimeOfReadFile;
        Long timeCostOfReadFile = ((long) 0);

//        DeserializationHelper deserializationHelper = new DeserializationHelper();
        BytesCounter counter = new BytesCounter();


        Long timeCostOfSearching = ((long) 0);

        Long timeCostOfDeserializationATree = ((long) 0);

        Long timeCostOfDeserializationALeaf = ((long) 0);
//        RandomAccessFile file = null;
//        for (String fileName : fileNames) {
            try {
//                FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                startTimeOfReadFile = System.currentTimeMillis();
                FileSystemHandler fileSystemHandler = new HdfsFileSystemHandler("/home/acelzj");
                timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                CacheMappingKey mappingKey = new CacheMappingKey(fileName, 0);

                BTree deserializedTree = null;
                if (cacheMapping.get(mappingKey) != null) {
                    deserializedTree = (BTree) cacheMapping.get(mappingKey).getCacheData().getData();
                } else {

                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.openFile("/", fileName);
                    timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;


                    byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];

                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.readBytesFromFile(0, serializedTree);
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                    Long startTimeOfDeserializationATree = System.currentTimeMillis();
                    deserializedTree = DeserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                    timeCostOfDeserializationATree = System.currentTimeMillis() - startTimeOfDeserializationATree;

                    CacheData cacheData = new TemplateCacheData(deserializedTree);
                    CacheUnit cacheUnit = new CacheUnit();
                    cacheUnit.setCacheData(cacheData);
                    cacheMapping.put(mappingKey, cacheUnit);


                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.closeFile();
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                }


                Long searchStartTime = System.currentTimeMillis();
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

                mappingKey = new CacheMappingKey(fileName, offset);

                BTreeLeafNode leaf;

                if (cacheMapping.get(mappingKey) != null) {
                    leaf = (BTreeLeafNode) cacheMapping.get(mappingKey).getCacheData().getData();
                } else {

                    byte[] lengthInByte = new byte[4];


                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.openFile("/", fileName);
                    timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;


//                    startTimeOfReadFile = System.currentTimeMillis();
//                    fileSystemHandler.seek(offset);
//                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.readBytesFromFile(offset, lengthInByte);
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                    int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();

                    byte[] leafInByte = new byte[lengthOfLeaveInBytes];


//                    startTimeOfReadFile = System.currentTimeMillis();
//                    fileSystemHandler.seek(offset + 4);
//                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.readBytesFromFile(offset + 4, leafInByte);
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                    Long startTimeOfDeserializationALeaf = System.nanoTime();
                    leaf = DeserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
                    timeCostOfDeserializationALeaf = System.nanoTime() - startTimeOfDeserializationALeaf;

                    CacheData cacheData = new LeafNodeCacheData(leaf);
                    CacheUnit cacheUnit = new CacheUnit();
                    cacheUnit.setCacheData(cacheData);
                    cacheMapping.put(mappingKey, cacheUnit);


                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.closeFile();
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);
                }

                searchStartTime = System.currentTimeMillis();
                ArrayList<byte[]> serializedTuples = leaf.searchAndGetTuplesWithinTimestampRange(
                        key, timestampLowerBound, timestampUpperBound);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

//                if (serializedTuples != null) {
//                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                            new Values(queryId, serializedTuples));


//                }

//                collector.emit(NormalDistributionIndexingTopology.TimeCostInformationStream,
//                        new Values(queryId, timeCostOfReadFile, timeCostOfDeserializationALeaf,
//                                timeCostOfDeserializationATree));

//                startTimeOfReadFile = System.currentTimeMillis();
//                fileSystemHandler.closeFile();
//                timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                metrics.setTotalTime(System.currentTimeMillis() - startTime);
                metrics.setFileReadingTime(timeCostOfReadFile);
                metrics.setLeafDeserializationTime(timeCostOfDeserializationALeaf);
                metrics.setTreeDeserializationTime(timeCostOfDeserializationATree);
                metrics.setSearchTime(timeCostOfSearching);

//                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                        new Values(queryId, serializedTuples, timeCostOfReadFile, timeCostOfDeserializationALeaf,
//                                timeCostOfDeserializationATree));
                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                        new Values(queryId, serializedTuples, metrics));

                collector.emit(NormalDistributionIndexingTopology.FileSubQueryFinishStream,
                        new Values("finished"));

                /*
                file = new RandomAccessFile("/home/acelzj/" + fileName, "r");
                byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];
                DeserializationHelper deserializationHelper = new DeserializationHelper();
                BytesCounter counter = new BytesCounter();

                file.read(serializedTree, 0, Config.TEMPLATE_SIZE);
                BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);
//                System.out.println("offset " + offset);
                byte[] lengthInByte = new byte[4];
                file.seek(offset);
                file.read(lengthInByte);
                int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
//                System.out.println("Length of leave in bytes " + lengthOfLeaveInBytes);
                byte[] leafInByte = new byte[lengthOfLeaveInBytes];
                file.seek(offset + 4);
                file.read(leafInByte);
                BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
//                System.out.println("The key is " + key);
//                System.out.println("The leaf is ");
//                deserializedLeaf.print();
                ArrayList<byte[]> serializedTuples = deserializedLeaf.searchAndGetTuples(key);
                if (serializedTuples != null) {
                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                            new Values(key, serializedTuples));
                }
                file.close();*/
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("key", "serializedTuples"));
        /*
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples"));*/

//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("queryId", "serializedTuples", "timeCostOfReadFile", "timeCostOfDeserializationALeaf",
//                        "timeCostOfDeserializationATree"));

//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.TimeCostInformationStream,
//                new Fields("queryId", "timeCostOfReadFile", "timeCostOfDeserializationALeaf",
//                        "timeCostOfDeserializationATree"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSubQueryFinishStream,
                new Fields("finished"));
    }
}
