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
import indexingTopology.NormalDistributionIndexingAndRangeQueryTopology;
import indexingTopology.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/15/16.
 */
public class RangeQueryChunkScannerBolt extends BaseRichBolt{

    OutputCollector collector;

    private int bTreeOder;

    private transient LRUCache<CacheMappingKey, CacheUnit> cacheMapping;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        bTreeOder = 4;
        cacheMapping = new LRUCache<CacheMappingKey, CacheUnit>(Config.CACHE_SIZE);
    }

    public void execute(Tuple tuple) {

        RangeQuerySubQuery subQuery = (RangeQuerySubQuery) tuple.getValue(0);

//        Long queryId = tuple.getLong(0);
//        Double leftKey = tuple.getDouble(1);
//        Double rightKey = tuple.getDouble(2);

//        ArrayList<String> fileNames = (ArrayList) tuple.getValue(2);
//        String fileName = (String) tuple.getValue(3);
//        Long timestampLowerBound = tuple.getLong(4);
//        Long timestampUpperBound = tuple.getLong(5);


        Long queryId = subQuery.getQueryId();
        Double leftKey = subQuery.getlefKey();
        Double rightKey = subQuery.getRightKey();
        String fileName = subQuery.getFileName();
        Long timestampLowerBound = subQuery.getStartTimestamp();
        Long timestampUpperBound = subQuery.getEndTimestamp();

        RandomAccessFile file = null;
        ArrayList<byte[]> serializedTuples = new ArrayList<byte[]>();

        FileScanMetrics metrics = new FileScanMetrics();

        Long startTime = System.currentTimeMillis();


        Long startTimeOfReadFile;
        Long timeCostOfReadFile = ((long) 0);

//        DeserializationHelper deserializationHelper = new DeserializationHelper();
        BytesCounter counter = new BytesCounter();


        Long timeCostOfSearching = ((long) 0);

        Long timeCostOfDeserializationATree = ((long) 0);

        Long timeCostOfDeserializationALeaf = ((long) 0);


//        for (String fileName : fileNames) {
            try {
//                FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                FileSystemHandler fileSystemHandler = new HdfsFileSystemHandler("/home/acelzj");
                CacheMappingKey mappingKey = new CacheMappingKey(fileName, 0);
                BTree deserializedTree = null;
                if (cacheMapping.get(mappingKey) != null) {
                    deserializedTree = (BTree) cacheMapping.get(mappingKey).getCacheData().getData();
                } else {

                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.openFile("/", fileName);
                    timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;

                    byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];
//                DeserializationHelper deserializationHelper = new DeserializationHelper();
//                BytesCounter counter = new BytesCounter();

                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.readBytesFromFile(0, serializedTree);
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                    Long startTimeOfDeserializationATree = System.currentTimeMillis();
//                    deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                    deserializedTree = DeserializationHelper.deserializeBTree(fileSystemHandler, bTreeOder, counter);
                    timeCostOfDeserializationATree = System.currentTimeMillis() - startTimeOfDeserializationATree;

                    CacheData cacheData = new TemplateCacheData(deserializedTree);
                    CacheUnit cacheUnit = new CacheUnit();
                    cacheUnit.setCacheData(cacheData);
                    cacheMapping.put(mappingKey, cacheUnit);

                    startTimeOfReadFile = System.currentTimeMillis();
                    fileSystemHandler.closeFile();
                    timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                }


                BTreeNode mostLeftNode = deserializedTree.findLeafNodeShouldContainKeyInDeserializedTemplate(leftKey);
                BTreeNode mostRightNode = deserializedTree.findLeafNodeShouldContainKeyInDeserializedTemplate(rightKey);

                Long searchStartTime = System.currentTimeMillis();
                List<Integer> offsets = deserializedTree.getOffsetsOfLeaveNodesShoulsContainKeys(mostLeftNode
                        , mostRightNode);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

//                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey);
//                int endPosition = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(rightKey);
//                long length = fileSystemHandler.getLengthOfFile("/", fileName);


                BTreeLeafNode leaf;

                for (Integer offset : offsets) {
//                while (offset < endPosition) {

                    mappingKey = new CacheMappingKey(fileName, offset);
                    if (cacheMapping.get(mappingKey) != null) {
                        leaf = (BTreeLeafNode) cacheMapping.get(mappingKey).getCacheData().getData();
                    } else {
                        byte[] lengthInByte = new byte[4];

                        startTimeOfReadFile = System.currentTimeMillis();
                        fileSystemHandler.openFile("/", fileName);
                        timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;

//                        startTimeOfReadFile = System.currentTimeMillis();
//                        fileSystemHandler.seek(offset);
//                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                        startTimeOfReadFile = System.currentTimeMillis();
                        fileSystemHandler.readBytesFromFile(offset, lengthInByte);
                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                        int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
                        if (lengthOfLeaveInBytes == 0) {
                            break;
                        }
                        byte[] leafInByte = new byte[lengthOfLeaveInBytes];


//                        startTimeOfReadFile = System.currentTimeMillis();
//                        fileSystemHandler.seek(offset + 4);
//                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                        startTimeOfReadFile = System.currentTimeMillis();
                        fileSystemHandler.readBytesFromFile(offset + 4, leafInByte);
                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

                        Long startTimeOfDeserializationALeaf = System.currentTimeMillis();
                        leaf = DeserializationHelper.deserializeLeaf(leafInByte,
                                bTreeOder, counter);
                        timeCostOfDeserializationALeaf += (System.currentTimeMillis() - startTimeOfDeserializationALeaf);

                        CacheData cacheData = new LeafNodeCacheData(leaf);
                        CacheUnit cacheUnit = new CacheUnit();
                        cacheUnit.setCacheData(cacheData);
                        cacheMapping.put(mappingKey, cacheUnit);

                        startTimeOfReadFile = System.currentTimeMillis();
                        fileSystemHandler.closeFile();
                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


                    }

                    searchStartTime = System.currentTimeMillis();
                    ArrayList<byte[]> tuples = leaf.rangeSearchAndGetTuples(timestampLowerBound, timestampUpperBound);
                    timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);
                    /*
                    if (tuples.size() == 0) {
                        break;
                    } else {
                        serializedTuples.addAll(tuples);
                    } */
                    if (tuples.size() != 0) {
                        serializedTuples.addAll(tuples);
                    }

//                    Double key = (Double) leaf.getKey(leaf.getKeyCount() - 1);
//                    offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key + 0.000001);
//                    offset = offset + lengthOfLeaveInBytes + 4;
                }

                /*
                startTimeOfReadFile = System.currentTimeMillis();
                fileSystemHandler.closeFile();
                timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);
                */


                metrics.setTotalTime(System.currentTimeMillis() - startTime);
                metrics.setFileReadingTime(timeCostOfReadFile);
                metrics.setLeafDeserializationTime(timeCostOfDeserializationALeaf);
                metrics.setTreeDeserializationTime(timeCostOfDeserializationATree);
                metrics.setSearchTime(timeCostOfSearching);

//                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
//                        new Values(queryId, serializedTuples, timeCostOfReadFile, timeCostOfDeserializationALeaf,
//                                timeCostOfDeserializationATree));

                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                        new Values(queryId, serializedTuples, metrics));








                /*
                file = new RandomAccessFile("/home/acelzj/" + fileName, "r");
                byte[] serializedTree = new byte[Config.TEMPLATE_SIZE];
                DeserializationHelper deserializationHelper = new DeserializationHelper();
                BytesCounter counter = new BytesCounter();

                file.read(serializedTree, 0, Config.TEMPLATE_SIZE);
                BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(leftKey);
                while (offset < file.length()) {
//                    System.out.println("Offset " + offset);
                    byte[] lengthInByte = new byte[4];
                    file.seek(offset);
                    file.read(lengthInByte);
                    int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
                    if (lengthOfLeaveInBytes == 0) {
                        break;
                    }
                    byte[] leafInByte = new byte[lengthOfLeaveInBytes];
                    file.seek(offset + 4);
                    file.read(leafInByte);
                    BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInByte,
                            bTreeOder, counter);
                    ArrayList<byte[]> tuples = deserializedLeaf.rangeSearchAndGetTuples(leftKey, rightKey);
                    if (tuples.size() == 0) {
                        break;
                    } else {
                        serializedTuples.addAll(tuples);
                    }
                    offset = offset + lengthOfLeaveInBytes + 4;

                }*/
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
//        }
//        System.out.println("The tuples are");
//        System.out.println(serializedTuples);
//        System.out.println("Size " + serializedTuples.size());
//        if (serializedTuples.size() != 0) {
//            collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                    new Values(queryId, serializedTuples));
//        }
//        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("leftKey", "rightKey", "serializedTuples"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
//                new Fields("queryId", "serializedTuples"));

//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("queryId", "serializedTuples", "timeCostOfReadFile", "timeCostOfDeserializationALeaf",
//                        "timeCostOfDeserializationATree"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSubQueryFinishStream,
                new Fields("finished"));

    }

}
