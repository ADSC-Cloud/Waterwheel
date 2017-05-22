package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.cache.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.streams.Streams;
import indexingTopology.util.*;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by acelzj on 11/15/16.
 */
public class ChunkScanner <TKey extends Number & Comparable<TKey>> extends BaseRichBolt {

    OutputCollector collector;

    private transient LRUCache<BlockId, CacheUnit> blockIdToCacheUnit;

    private transient Kryo kryo;

    private DataSchema schema;

    private static final Logger LOG = LoggerFactory.getLogger(ChunkScanner.class);

    private transient ArrayBlockingQueue<SubQuery<TKey>> pendingQueue;

    private transient Semaphore subQueryHandlingSemaphore;

//    Long startTime;

//    Long timeCostOfReadFile;

//    Long timeCostOfSearching;

//    Long timeCostOfDeserializationATree;

//    Long timeCostOfDeserializationALeaf;
    private Thread subQueryHandlingThread;

    TopologyConfig config;

    public ChunkScanner(DataSchema schema, TopologyConfig config) {
        this.schema = schema;
        this.config = config;
    }

    private Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) throws IOException {
//        fileSystemHandler.openFile("/", fileName);
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int templateLength = input.readInt();



        bytesToRead = new byte[templateLength];

        if (!config.HDFSFlag)
            fileSystemHandler.seek(4);
        fileSystemHandler.readBytesFromFile(4, bytesToRead);


        input = new Input(bytesToRead);
        BTree template = kryo.readObject(input, BTree.class);


//        fileSystemHandler.closeFile();

        return new Pair(template, templateLength);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        blockIdToCacheUnit = new LRUCache<>(config.CACHE_SIZE);

        pendingQueue = new ArrayBlockingQueue<>(1024);

        subQueryHandlingSemaphore = new Semaphore(1);

        createSubQueryHandlingThread();

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {

            SubQueryOnFile subQuery = (SubQueryOnFile) tuple.getValueByField("subquery");

            try {
//                System.out.println("query id " + subQuery.getQueryId() + " has been put to the queue");
                pendingQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceStreamId().equals(Streams.SubQueryReceivedStream)) {
            subQueryHandlingSemaphore.release();
//            System.out.println(Streams.SubQueryReceivedStream);
        }
//        LOG.info("Subquery time : " + (System.currentTimeMillis() - start));
    }

    private void createSubQueryHandlingThread() {
        subQueryHandlingThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        subQueryHandlingSemaphore.acquire();
                        SubQueryOnFile subQuery = (SubQueryOnFile) pendingQueue.take();
//                        System.out.println("sub query " + subQuery.getQueryId() + " has been taken from queue");

                        handleSubQuery(subQuery);
//                        System.out.println("sub query " + subQuery.getQueryId() + " has been processed");
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        subQueryHandlingThread.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream,
                new Fields("queryId", "serializedTuples", "metrics"));

        outputFieldsDeclarer.declareStream(Streams.FileSubQueryFinishStream,
                new Fields("finished"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        subQueryHandlingThread.interrupt();
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

//        metrics.setFileName(fileName);

        long start = System.currentTimeMillis();

//        metrics.setSubqueryStartTime(start);

//        long fileTime = 0;
//        long fileStart = System.currentTimeMillis();
        FileSystemHandler fileSystemHandler = null;
        if (config.HDFSFlag) {
            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
        } else {
            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
        }
        fileSystemHandler.openFile("/", fileName);
//        fileTime += (System.currentTimeMillis() - fileStart);

//        long readTemplateStart = System.currentTimeMillis();
        Pair data = getTemplateData(fileSystemHandler, fileName);
//        long temlateRead = System.currentTimeMillis() - readTemplateStart;

        BTree template = (BTree) data.getKey();
        Integer length = (Integer) data.getValue();

        
        BTreeLeafNode leaf = null;
        ArrayList<byte[]> tuples = new ArrayList<byte[]>();


//        long searchOffsetsStart = System.currentTimeMillis();
        List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);

//        System.out.println("template");
//        System.out.println(offsets);
//        template.printBtree();
//        metrics.setSearchTime(System.currentTimeMillis() - searchOffsetsStart);

//        long readLeafBytesStart = System.currentTimeMillis();
//        byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length);
        byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length, metrics);
//        long leafBytesReadingTime = System.currentTimeMillis() - readLeafBytesStart;
//        metrics.setLeafBytesReadingTime(leafBytesReadingTime);


//        long totalTupleGet = 0L;
//        long totalLeafRead = 0L;
        Input input = new Input(bytesToRead);


        for (Integer offset : offsets) {
            BlockId blockId = new BlockId(fileName, offset + length + 4);

            long readLeaveStart = System.currentTimeMillis();
            leaf = (BTreeLeafNode) getFromCache(blockId);

            if (leaf == null) {
                leaf = getLeafFromExternalStorage(fileSystemHandler, input);
                CacheData cacheData = new LeafNodeCacheData(leaf);
                putCacheData(blockId, cacheData);
            }
//            totalLeafRead += (System.currentTimeMillis() - readLeaveStart);

            long tupleGetStart = System.currentTimeMillis();
            ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);
            //deserialize
            List<DataTuple> dataTuples = new ArrayList<>();
            tuplesInKeyRange.stream().forEach(e -> dataTuples.add(schema.deserializeToDataTuple(e)));

            //filter by timestamp range
            filterByTimestamp(dataTuples, timestampLowerBound, timestampUpperBound);

            //filter by predicate
//            System.out.println("Before predicates: " + dataTuples.size());
            filterByPredicate(dataTuples, subQuery.getPredicate());
//            System.out.println("After predicates: " + dataTuples.size());

            if (subQuery.getAggregator() != null) {
                Aggregator.IntermediateResult intermediateResult = subQuery.getAggregator().createIntermediateResult();
                subQuery.getAggregator().aggregate(dataTuples, intermediateResult);
                dataTuples.clear();
                dataTuples.addAll(subQuery.getAggregator().getResults(intermediateResult).dataTuples);
            }

//            totalTupleGet += (System.currentTimeMillis() - tupleGetStart);

            //serialize
            List<byte[]> serializedDataTuples = new ArrayList<>();

            if (subQuery.getAggregator() != null) {
                DataSchema outputSchema = subQuery.getAggregator().getOutputDataSchema();
                dataTuples.stream().forEach(p -> serializedDataTuples.add(outputSchema.serializeTuple(p)));
            } else {
                dataTuples.stream().forEach(p -> serializedDataTuples.add(schema.serializeTuple(p)));
            }


            tuples.addAll(serializedDataTuples);
        }

        long closeStart = System.currentTimeMillis();
        fileSystemHandler.closeFile();
//        fileTime += (System.currentTimeMillis() - closeStart);

//        metrics.setSubqueryEndTime(System.currentTimeMillis());


//        System.out.println("total time " + (System.currentTimeMillis() - start));
//        System.out.println("file open and close time " + fileTime);
//        System.out.println("template read " + temlateRead);
//        System.out.println("leaf read " + totalLeafRead);
//        System.out.println("total tuple get" + totalTupleGet);

//        metrics.setTotalTime(System.currentTimeMillis() - start);
//        metrics.setFileOpenAndCloseTime(fileTime);
//        metrics.setLeafReadTime(totalLeafRead);
//        metrics.setTupleGettingTime(totalTupleGet);
//        metrics.setTemplateReadingTime(temlateRead);

//        collector.emit(Streams.FileSystemQueryStream, new Values(queryId, tuples, metrics));
//        metrics.setNumberOfRecords((long) tuples.size());
//        System.out.println(tuples.size());

        metrics.setTotalTime(System.currentTimeMillis() - start);


//        tuples.clear();
//        System.out.println(String.format("%d tuples are found on file %s", tuples.size(), fileName));
        collector.emit(Streams.FileSystemQueryStream, new Values(subQuery, tuples, metrics));


        if (!config.SHUFFLE_GROUPING_FLAG) {
            collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
        }
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

    private void filterByTimestamp(List<DataTuple> tuples, Long timestampLowerBound, Long timestampUpperBound)
            throws IOException {

        List<DataTuple> result =
        tuples.stream().filter(p -> {
            Long timestamp = (Long) schema.getValue("timestamp", p);
            return timestampLowerBound <= timestamp && timestampUpperBound >= timestamp;
        }).collect(Collectors.toList());
        tuples.clear();
        tuples.addAll(result);

    }

    private void filterByPredicate(List<DataTuple> tuples, Predicate<DataTuple> predicate) {
        if (predicate != null) {
            List<DataTuple> result = tuples.stream().filter(predicate).collect(Collectors.toList());
            tuples.clear();
            tuples.addAll(result);
        }
    }


    private BTreeLeafNode getLeafFromExternalStorage(FileSystemHandler fileSystemHandler, Input input)
            throws IOException {
        input.setPosition(input.position() + 4);
        BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);

        return leaf;
    }


    private Pair getTemplateData(FileSystemHandler fileSystemHandler, String fileName) {
        Pair data = null;
        try {
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


    private byte[] readLeafBytesFromFile(FileSystemHandler fileSystemHandler, List<Integer> offsets, int length, FileScanMetrics fileScanMetrics) throws IOException {
        Integer endOffset = offsets.get(offsets.size() - 1);
        Integer startOffset = offsets.get(0);

        byte[] bytesToRead = new byte[4];

        Long startTime = System.currentTimeMillis();

        //code below used to change the position of file pointer in local file system
        if (!config.HDFSFlag)
            fileSystemHandler.seek(endOffset + length + 4);

        fileSystemHandler.readBytesFromFile(endOffset + length + 4, bytesToRead);

        Input input = new Input(bytesToRead);
        int lastLeafLength = input.readInt();

        fileScanMetrics.setLengthReadTime(System.currentTimeMillis() - startTime);

        int totalLength = lastLeafLength + (endOffset - offsets.get(0)) + 4;

        bytesToRead = new byte[totalLength];
        startTime = System.currentTimeMillis();

        ////code below used to change the position of file pointer in local file system
        if (!config.HDFSFlag)
            fileSystemHandler.seek(startOffset + length + 4);

        fileSystemHandler.readBytesFromFile(startOffset + length + 4, bytesToRead);
        fileScanMetrics.setTotalBytesReadTime(System.currentTimeMillis() - startTime);

        return bytesToRead;
    }

}