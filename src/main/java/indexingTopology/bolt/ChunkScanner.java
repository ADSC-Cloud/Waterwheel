package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.DataSchema;
import indexingTopology.DataTuple;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.cache.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.streams.Streams;
import indexingTopology.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/15/16.
 */
public class ChunkScanner <TKey extends Comparable<TKey>> extends BaseRichBolt {

    OutputCollector collector;

    private transient LRUCache<BlockId, CacheUnit> blockIdToCacheUnit;

    private transient Kryo kryo;

    private DataSchema schema;

//    Long startTime;

//    Long timeCostOfReadFile;

//    Long timeCostOfSearching;

//    Long timeCostOfDeserializationATree;

//    Long timeCostOfDeserializationALeaf;


    public ChunkScanner(DataSchema schema) {
        this.schema = schema;
    }

    private Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) throws IOException {
        fileSystemHandler.openFile("/", fileName);
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int templateLength = input.readInt();



        bytesToRead = new byte[templateLength];
//        fileSystemHandler.seek(4);
        fileSystemHandler.readBytesFromFile(4, bytesToRead);


        input = new Input(bytesToRead);
        BTree template = kryo.readObject(input, BTree.class);


        fileSystemHandler.closeFile();

        return new Pair(template, templateLength);
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        blockIdToCacheUnit = new LRUCache<BlockId, CacheUnit>(TopologyConfig.CACHE_SIZE);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());
    }

    public void execute(Tuple tuple) {

        SubQueryOnFile subQuery = (SubQueryOnFile) tuple.getValueByField("subquery");

//        timeCostOfReadFile = ((long) 0);
//
//        timeCostOfSearching = ((long) 0);
//
//        timeCostOfDeserializationATree = ((long) 0);
//
//        timeCostOfDeserializationALeaf = ((long) 0);

        try {
            handleSubQuery(subQuery);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(Streams.FileSubQueryFinishStream,
                new Fields("finished"));
    }

    @SuppressWarnings("unchecked")
    private void handleSubQuery(SubQueryOnFile subQuery) throws IOException {

        Long queryId = subQuery.getQueryId();
        TKey leftKey =  (TKey) subQuery.getLeftKey();
        TKey rightKey =  (TKey) subQuery.getRightKey();
        String fileName = subQuery.getFileName();
        Long timestampLowerBound = subQuery.getStartTimestamp();
        Long timestampUpperBound = subQuery.getEndTimestamp();

        FileScanMetrics metrics = new FileScanMetrics();


        Pair data = getTemplateData(fileName);
        BTree template = (BTree) data.getKey();
        Integer length = (Integer) data.getValue();


//        BTreeNode mostLeftNode = template.findInnerNodeShouldContainKey(leftKey);
//        BTreeNode mostRightNode = template.findInnerNodeShouldContainKey(rightKey);


        BTreeLeafNode leaf;
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();
        List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);
        for (Integer offset : offsets) {
            BlockId blockId = new BlockId(fileName, offset + length + 4);

            leaf = (BTreeLeafNode) getFromCache(blockId);
            if (leaf == null) {
                leaf = getLeafFromExternalStorage(fileName, offset + length + 4);
                CacheData cacheData = new LeafNodeCacheData(leaf);
                putCacheData(blockId, cacheData);
            }

            ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);
            ArrayList<byte[]> tuplesWithinTimestamp = getTuplesWithinTimestamp(tuplesInKeyRange, timestampLowerBound, timestampUpperBound);

            if (tuplesWithinTimestamp.size() != 0) {
                tuples.addAll(tuplesWithinTimestamp);
            }
        }

        collector.emit(Streams.FileSystemQueryStream, new Values(queryId, tuples, metrics));

        collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
    }


    private Object getFromCache(BlockId blockId) {
        if (blockIdToCacheUnit.get(blockId) == null) {
            return null;
        }
        return blockIdToCacheUnit.get(blockId).getCacheData().getData();
    }


    private void putCacheData(BlockId blockId, CacheData cacheData) {
        CacheUnit cacheUnit = new CacheUnit();
        cacheUnit.setCacheData(cacheData);
        blockIdToCacheUnit.put(blockId, cacheUnit);
    }

    private ArrayList<byte[]> getTuplesWithinTimestamp(ArrayList<byte[]> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        ArrayList<byte[]> tuplesWithinTimestamp = new ArrayList<>();
        for (int i = 0; i < tuples.size(); ++i) {
            DataTuple dataTuple = schema.deserializeToDataTuple(tuples.get(i));
            Long timestamp = (Long) schema.getValue("timestamp", dataTuple);
            if (timestampLowerBound <= timestamp && timestampUpperBound >= timestamp) {
                tuplesWithinTimestamp.add(tuples.get(i));
            }
        }

        return tuplesWithinTimestamp;
    }

    private BTreeLeafNode getLeafFromExternalStorage(String fileName, int offset)
            throws IOException {

        FileSystemHandler fileSystemHandler = null;
        if (TopologyConfig.HDFSFlag) {
            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
        }

        byte[] bytesToRead = new byte[4];
        fileSystemHandler.openFile("/", fileName);
        fileSystemHandler.seek(offset);
        fileSystemHandler.readBytesFromFile(offset, bytesToRead);


        Input input = new Input(bytesToRead);
        int leaveNodeLength = input.readInt();

        bytesToRead = new byte[leaveNodeLength];
        fileSystemHandler.readBytesFromFile(offset + 4, bytesToRead);

//        fileSystemHandler.seek(offset + 4);

        input = new Input(bytesToRead);
        BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);


        fileSystemHandler.closeFile();

        return leaf;
    }


    private Pair getTemplateData(String fileName) {
        Pair data = null;
        try {
            FileSystemHandler fileSystemHandler = null;
            if (TopologyConfig.HDFSFlag) {
                fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
            } else {
                fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
            }

            BlockId blockId = new BlockId(fileName, 0);

            data = (Pair) getFromCache(blockId);
            if (data == null) {
                data = getTemplateFromExternalStorage(fileSystemHandler, fileName);
                CacheData cacheData = new TemplateCacheData(data);
                putCacheData(blockId, cacheData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}