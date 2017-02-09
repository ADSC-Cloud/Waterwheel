package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.DataSchema;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/15/16.
 */
public class ChunkScanner <TKey extends Comparable<TKey>> extends BaseRichBolt{

    OutputCollector collector;

    private transient LRUCache<CacheMappingKey, CacheUnit> cacheMapping;

    private transient Kryo kryo;

    private DataSchema schema;

    Long startTime;

    Long timeCostOfReadFile;

    Long timeCostOfSearching;

    Long timeCostOfDeserializationATree;

    Long timeCostOfDeserializationALeaf;


    public ChunkScanner(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        cacheMapping = new LRUCache<CacheMappingKey, CacheUnit>(TopologyConfig.CACHE_SIZE);
        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());
    }

    public void execute(Tuple tuple) {

        SubQuery subQuery = (SubQuery) tuple.getValue(0);

        timeCostOfReadFile = ((long) 0);

        timeCostOfSearching = ((long) 0);

        timeCostOfDeserializationATree = ((long) 0);

        timeCostOfDeserializationALeaf = ((long) 0);

        try {
            executeRangeQuery(subQuery);
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
    private void executeRangeQuery(SubQuery subQuery) throws IOException {

        Long queryId = subQuery.getQueryId();
        TKey leftKey =  (TKey) subQuery.getLeftKey();
        TKey rightKey =  (TKey) subQuery.getRightKey();
        String fileName = subQuery.getFileName();
        Long timestampLowerBound = subQuery.getStartTimestamp();
        Long timestampUpperBound = subQuery.getEndTimestamp();

        FileScanMetrics metrics = new FileScanMetrics();

        ArrayList<byte[]> serializedTuples = new ArrayList<byte[]>();

        Pair data = getTemplateData(fileName);

        BTree template = (BTree) data.getKey();

        Integer length = (Integer) data.getValue();

        BTreeNode mostLeftNode = template.findLeafNodeShouldContainKeyInDeserializedTemplate(leftKey);

        BTreeNode mostRightNode = template.findLeafNodeShouldContainKeyInDeserializedTemplate(rightKey);

        List<Integer> offsets = template.getOffsetsOfLeaveNodesShouldContainKeys(mostLeftNode, mostRightNode);

        BTreeLeafNode leaf;

        for (Integer offset : offsets) {
            CacheMappingKey mappingKey = new CacheMappingKey(fileName, offset + length + 4);
            leaf = (BTreeLeafNode) getFromCache(mappingKey);
            if (leaf == null) {
                leaf = getLeafFromExternalStorage(fileName, offset + length + 4);
                CacheData cacheData = new LeafNodeCacheData(leaf);
                putCacheData(cacheData, mappingKey);
            }

            ArrayList<byte[]> tuplesInKeyRange = leaf.getTuples(leftKey, rightKey);

            ArrayList<byte[]> tuplesWithinTimestamp = getTuplesWithinTimestamp(tuplesInKeyRange, timestampLowerBound, timestampUpperBound);

            if (tuplesWithinTimestamp.size() != 0) {
                serializedTuples.addAll(tuplesWithinTimestamp);
            }
        }

        collector.emit(Streams.FileSystemQueryStream, new Values(queryId, serializedTuples, metrics));

        collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));

    }

    private Object getFromCache(CacheMappingKey mappingKey) {
        if (cacheMapping.get(mappingKey) == null) {
            return null;
        }
        return cacheMapping.get(mappingKey).getCacheData().getData();
    }


    private Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) {
        fileSystemHandler.openFile("/", fileName);

        byte[] templateLengthInBytes = new byte[4];
        fileSystemHandler.readBytesFromFile(templateLengthInBytes);

        Input input = new Input(templateLengthInBytes);
        int length = input.readInt();

        byte[] serializedTemplate = new byte[length];

        fileSystemHandler.readBytesFromFile(0, serializedTemplate);

        input = new Input(serializedTemplate);
        BTree template = kryo.readObject(input, BTree.class);

        fileSystemHandler.closeFile();

        return new Pair(template, length);
    }


    private void putCacheData(CacheData cacheData, CacheMappingKey mappingKey) {
        CacheUnit cacheUnit = new CacheUnit();
        cacheUnit.setCacheData(cacheData);
        cacheMapping.put(mappingKey, cacheUnit);
    }

    private ArrayList<byte[]> getTuplesWithinTimestamp(ArrayList<byte[]> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        ArrayList<byte[]> serializedTuples = new ArrayList<>();

        for (int i = 0; i < tuples.size(); ++i) {
            Values deserializedTuple = schema.deserialize(tuples.get(i));
            if (timestampLowerBound <= (Long) deserializedTuple.get(schema.getNumberOfFields()) &&
                    timestampUpperBound >= (Long) deserializedTuple.get(schema.getNumberOfFields())) {
                serializedTuples.add(tuples.get(i));
            }
        }

        return serializedTuples;
    }

    private BTreeLeafNode getLeafFromExternalStorage(String fileName, int offset)
            throws IOException {

        FileSystemHandler fileSystemHandler = null;
        if (TopologyConfig.HDFSFlag) {
            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
        }

        byte[] lengthInByte = new byte[4];
        fileSystemHandler.openFile("/", fileName);

        fileSystemHandler.seek(offset);

        fileSystemHandler.readBytesFromFile(offset, lengthInByte);

        int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInByte, 0, 4).getInt();

        byte[] leafInByte = new byte[lengthOfLeaveInBytes+1];

        fileSystemHandler.seek(offset + 4);

        fileSystemHandler.readBytesFromFile(offset + 4, leafInByte);

        Input input = new Input(leafInByte);
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

            CacheMappingKey mappingKey = new CacheMappingKey(fileName, 0);

            data = (Pair) getFromCache(mappingKey);

            if (data == null) {
                data = getTemplateFromExternalStorage(fileSystemHandler, fileName);
                CacheData cacheData = new TemplateCacheData(data);
                putCacheData(cacheData, mappingKey);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}