package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Cache.*;
import indexingTopology.Config.TopologyConfig;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.Streams.Streams;
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
        cacheMapping = new LRUCache<CacheMappingKey, CacheUnit>(TopologyConfig.CACHE_SIZE);
    }

    public void execute(Tuple tuple) {

        SubQuery subQuery = (SubQuery) tuple.getValue(0);

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
                FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                CacheMappingKey mappingKey = new CacheMappingKey(fileName, 0);
                BTree deserializedTree = (BTree) getFromCache(mappingKey);
                if (deserializedTree == null) {
                    deserializedTree = getTemplateFromExternalStorage(fileSystemHandler, fileName);

                    CacheData cacheData = new TemplateCacheData(deserializedTree);

                    putCacheData(cacheData, mappingKey);
                }

                Long searchStartTime = System.currentTimeMillis();
                int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

                mappingKey = new CacheMappingKey(fileName, offset);

                BTreeLeafNode leaf;

                mappingKey = new CacheMappingKey(fileName, offset);
                leaf = (BTreeLeafNode) getFromCache(mappingKey);
                if (leaf == null) {
                    leaf = getLeafFromExternalStorage(fileSystemHandler, fileName, offset);
                } else {
                    CacheData cacheData = new LeafNodeCacheData(leaf);
                    putCacheData(cacheData, mappingKey);
                }

                searchStartTime = System.currentTimeMillis();
                ArrayList<byte[]> serializedTuples = leaf.searchAndGetTuplesWithinTimestampRange(
                        key, timestampLowerBound, timestampUpperBound);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

                metrics.setTotalTime(System.currentTimeMillis() - startTime);
                metrics.setFileReadingTime(timeCostOfReadFile);
                metrics.setLeafDeserializationTime(timeCostOfDeserializationALeaf);
                metrics.setTreeDeserializationTime(timeCostOfDeserializationATree);
                metrics.setSearchTime(timeCostOfSearching);

//                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                        new Values(queryId, serializedTuples, timeCostOfReadFile, timeCostOfDeserializationALeaf,
//                                timeCostOfDeserializationATree));
                collector.emit(Streams.FileSystemQueryStream,
                        new Values(queryId, serializedTuples, metrics));

                collector.emit(Streams.FileSubQueryFinishStream,
                        new Values("finished"));
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

        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(Streams.FileSubQueryFinishStream,
                new Fields("finished"));
    }


    private Object getFromCache(CacheMappingKey mappingKey) {
        if (cacheMapping.get(mappingKey) == null) {
            return null;
        }
        return cacheMapping.get(mappingKey).getCacheData().getData();
    }

    private BTree getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) {
//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.openFile("/", fileName);
//        timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;

        byte[] serializedTree = new byte[TopologyConfig.TEMPLATE_SIZE];
//                DeserializationHelper deserializationHelper = new DeserializationHelper();
        BytesCounter counter = new BytesCounter();

//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(0, serializedTree);
//        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

//        Long startTimeOfDeserializationATree = System.currentTimeMillis();
        BTree deserializedTree = DeserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
//        timeCostOfDeserializationATree = System.currentTimeMillis() - startTimeOfDeserializationATree;
//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.closeFile();
//        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        return deserializedTree;
    }

    private void putCacheData(CacheData cacheData, CacheMappingKey mappingKey) {
        CacheUnit cacheUnit = new CacheUnit();
        cacheUnit.setCacheData(cacheData);
        cacheMapping.put(mappingKey, cacheUnit);
    }


    private BTreeLeafNode getLeafFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName, int offset)
            throws IOException {
        byte[] lengthInByte = new byte[4];
//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.openFile("/", fileName);
//        timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;
//                        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.seek(offset);
//                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);
//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(offset, lengthInByte);
//        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);
        int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
        byte[] leafInByte = new byte[lengthOfLeaveInBytes];
//                        startTimeOfReadFile = System.currentTimeMillis();
//                        fileSystemHandler.seek(offset + 4);
//                        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


//        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(offset + 4, leafInByte);
//        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);
//        Long startTimeOfDeserializationALeaf = System.currentTimeMillis();
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = DeserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
        fileSystemHandler.closeFile();
        return leaf;
//        timeCostOfDeserializationALeaf += (System.currentTimeMillis() - startTimeOfDeserializationALeaf);
    }

}
