package indexingTopology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.Cache.*;
import indexingTopology.Config.TopologyConfig;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.Streams.Streams;
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


    Long startTime;

    Long timeCostOfReadFile;

    Long timeCostOfSearching;

    Long timeCostOfDeserializationATree;

    Long timeCostOfDeserializationALeaf;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        bTreeOder = 4;
        cacheMapping = new LRUCache<CacheMappingKey, CacheUnit>(TopologyConfig.CACHE_SIZE);
    }

    public void execute(Tuple tuple) {

        RangeQuerySubQuery subQuery = (RangeQuerySubQuery) tuple.getValue(0);

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

        timeCostOfReadFile = ((long) 0);

        timeCostOfSearching = ((long) 0);

        timeCostOfDeserializationATree = ((long) 0);

        timeCostOfDeserializationALeaf = ((long) 0);

        try {
            FileSystemHandler fileSystemHandler = null;
            if (TopologyConfig.HDFSFlag) {
                fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
            } else {
                fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);

            }

            CacheMappingKey mappingKey = new CacheMappingKey(fileName, 0);
            BTree deserializedTree = (BTree) getFromCache(mappingKey);
            if (deserializedTree == null) {
                deserializedTree = getTemplateFromExternalStorage(fileSystemHandler, fileName);

                CacheData cacheData = new TemplateCacheData(deserializedTree);

                putCacheData(cacheData, mappingKey);
            }


            BTreeNode mostLeftNode = deserializedTree.findLeafNodeShouldContainKeyInDeserializedTemplate(leftKey);
            BTreeNode mostRightNode = deserializedTree.findLeafNodeShouldContainKeyInDeserializedTemplate(rightKey);

            Long searchStartTime = System.currentTimeMillis();
            List<Integer> offsets = deserializedTree.getOffsetsOfLeaveNodesShouldContainKeys(mostLeftNode
                        , mostRightNode);
            timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);

            BTreeLeafNode leaf;

            for (Integer offset : offsets) {
                mappingKey = new CacheMappingKey(fileName, offset);
                leaf = (BTreeLeafNode) getFromCache(mappingKey);
                if (leaf == null) {
                    leaf = getLeafFromExternalStorage(fileSystemHandler, fileName, offset);
                } else {
                    CacheData cacheData = new LeafNodeCacheData(leaf);
                    putCacheData(cacheData, mappingKey);
                }
                searchStartTime = System.currentTimeMillis();

                ArrayList<byte[]> tuples = getTuplesWithinTimeStamp(leaf, timestampLowerBound,
                        timestampUpperBound);

//                ArrayList<byte[]> tuples = leaf.rangeSearchAndGetTuples(timestampLowerBound, timestampUpperBound);
                timeCostOfSearching += (System.currentTimeMillis() - searchStartTime);
                if (tuples.size() != 0) {
                    serializedTuples.addAll(tuples);
                }
            }
            metrics.setTotalTime(System.currentTimeMillis() - startTime);
            metrics.setFileReadingTime(timeCostOfReadFile);
            metrics.setLeafDeserializationTime(timeCostOfDeserializationALeaf);
            metrics.setTreeDeserializationTime(timeCostOfDeserializationATree);
            metrics.setSearchTime(timeCostOfSearching);
            collector.emit(Streams.FileSystemQueryStream, new Values(queryId, serializedTuples, metrics));

//            collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("leftKey", "rightKey", "serializedTuples"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
//                new Fields("queryId", "serializedTuples"));

//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("queryId", "serializedTuples", "timeCostOfReadFile", "timeCostOfDeserializationALeaf",
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
        Long startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.openFile("/", fileName);
        timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;

        byte[] serializedTree = new byte[TopologyConfig.TEMPLATE_SIZE];

        BytesCounter counter = new BytesCounter();

        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(0, serializedTree);
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        Long startTimeOfDeserializationATree = System.currentTimeMillis();
          BTree deserializedTree = DeserializationHelper.deserializeBTree(serializedTree, bTreeOder, counter);
        timeCostOfDeserializationATree = System.currentTimeMillis() - startTimeOfDeserializationATree;

        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.closeFile();
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        return deserializedTree;
    }

    private void putCacheData(CacheData cacheData, CacheMappingKey mappingKey) {
        CacheUnit cacheUnit = new CacheUnit();
        cacheUnit.setCacheData(cacheData);
        cacheMapping.put(mappingKey, cacheUnit);
    }


    private ArrayList<byte[]> getTuplesWithinTimeStamp(BTreeLeafNode leaf, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        ArrayList<byte[]> serializedTuples = new ArrayList<>();

        ArrayList<byte[]> tuples = leaf.getTuples();

        for (int i = 0; i < tuples.size(); ++i) {
            Values deserializedTuple = DeserializationHelper.deserialize(tuples.get(i));
            if (timestampLowerBound <= (Long) deserializedTuple.get(3) &&
                    timestampUpperBound >= (Long) deserializedTuple.get(3)) {
                serializedTuples.add(tuples.get(i));
            }
        }

        return serializedTuples;
    }


    private BTreeLeafNode getLeafFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName, int offset)
            throws IOException {
        byte[] lengthInByte = new byte[4];
        Long startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.openFile("/", fileName);
        timeCostOfReadFile = System.currentTimeMillis() - startTimeOfReadFile;

        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.seek(offset);
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(offset, lengthInByte);
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();
        byte[] leafInByte = new byte[lengthOfLeaveInBytes];

        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.seek(offset + 4);
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);


        startTimeOfReadFile = System.currentTimeMillis();
        fileSystemHandler.readBytesFromFile(offset + 4, leafInByte);
        timeCostOfReadFile += (System.currentTimeMillis() - startTimeOfReadFile);

        Long startTimeOfDeserializationALeaf = System.currentTimeMillis();
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = DeserializationHelper.deserializeLeaf(leafInByte, bTreeOder, counter);
        timeCostOfDeserializationALeaf += (System.currentTimeMillis() - startTimeOfDeserializationALeaf);

        fileSystemHandler.closeFile();
        return leaf;

    }
}
