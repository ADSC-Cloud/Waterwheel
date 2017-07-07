package indexingTopology.bolt;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.bolt.metrics.LocationInfo;
import indexingTopology.cache.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.metrics.TaggedTimeMetrics;
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by acelzj on 11/15/16.
 */
public class ChunkScanner <TKey extends Number & Comparable<TKey>> extends BaseRichBolt {

    OutputCollector collector;

    private transient LRUCache<String, byte[]> chunkNameToChunkMapping;

    private transient LRUCache<BlockId, CacheUnit> blockIdToCacheUnit;

    private transient Kryo kryo;

    private DataSchema schema;

    private static final Logger LOG = LoggerFactory.getLogger(ChunkScanner.class);

    private transient ArrayBlockingQueue<SubQuery<TKey>> pendingQueue;

    private transient Semaphore subQueryHandlingSemaphore;

    private Thread subQueryHandlingThread;

    private Thread locationReportingThread;

    private Thread debugThread;

    private DebugInfo debugInfo;

    public static class DebugInfo {
        public String runningPosition;
    }

    TopologyConfig config;

    private SubqueryHandler subqueryHandler;



    public ChunkScanner(DataSchema schema, TopologyConfig config) {
        this.schema = schema;
        this.config = config;
    }

    private Pair getTemplateFromExternalStorage(FileSystemHandler fileSystemHandler, String fileName) throws IOException {
        byte[] bytesToRead = new byte[4];
        fileSystemHandler.readBytesFromFile(0, bytesToRead);

        Input input = new Input(bytesToRead);
        int templateLength = input.readInt();


        bytesToRead = new byte[templateLength];
        fileSystemHandler.readBytesFromFile(4, bytesToRead);

        input = new Input(bytesToRead);
        BTree template = kryo.readObject(input, BTree.class);


//        fileSystemHandler.closeFile();

        return new Pair(template, templateLength);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        chunkNameToChunkMapping = new LRUCache<>(config.CACHE_SIZE);

        blockIdToCacheUnit = new LRUCache<>(config.CACHE_SIZE);

        pendingQueue = new ArrayBlockingQueue<>(1024);

        subQueryHandlingSemaphore = new Semaphore(1);

        createSubQueryHandlingThread();

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));
        subqueryHandler = new SubqueryHandler(schema, config);

        locationReportingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                String hostName = "unknown";
                try {
                    hostName = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                LocationInfo info = new LocationInfo(LocationInfo.Type.Query, topologyContext.getThisTaskId(), hostName);
                outputCollector.emit(Streams.LocationInfoUpdateStream, new Values(info));

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        locationReportingThread.start();


        debugInfo = new DebugInfo();

        debugThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    System.out.println("Debugging thread terminates.");
                    break;
                }
                System.out.println(String.format("Debug info: %s", debugInfo.runningPosition));
            }
        });
        debugThread.start();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {

            SubQueryOnFile subQuery = (SubQueryOnFile) tuple.getValueByField("subquery");

            try {
                while (!pendingQueue.offer(subQuery, 1, TimeUnit.SECONDS)) {
                    System.out.println("Fail to offer a subquery to pending queue. Will retry.");
                }
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

                        System.out.println("$$$ to process a subquery on " + subQuery.getFileName() + " for " + subQuery.queryId);
                        if (config.ChunkOrientedCaching) {
                            List<byte[]> tuples = subqueryHandler.handleSubquery(subQuery, debugInfo);

                            collector.emit(Streams.FileSystemQueryStream, new Values(subQuery, tuples, new TaggedTimeMetrics()));

                            if (!config.SHUFFLE_GROUPING_FLAG) {
                                collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
                            }
                        } else {
                            handleSubQuery(subQuery);
                        }
                        System.out.println("$$$ processed a subquery on " + subQuery.getFileName() + " for " + subQuery.queryId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.out.println("subqueryHandlding thread is interrupted.");
                        break;
                    } catch (Exception e) {
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

        outputFieldsDeclarer.declareStream(Streams.LocationInfoUpdateStream, new Fields("info"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        subQueryHandlingThread.interrupt();
        locationReportingThread.interrupt();
        debugThread.interrupt();
    }

    @SuppressWarnings("unchecked")
    private void handleSubQuery(SubQueryOnFile subQuery) throws IOException {
//        ArrayList<byte[]> tuples = new ArrayList<>();
        List<DataTuple> dataTuplesInKeyRange = new ArrayList<>();
        TaggedTimeMetrics metrics = new TaggedTimeMetrics();
        List<byte[]> serializedDataTuples = new ArrayList<>();

        metrics.setTag("location", InetAddress.getLocalHost().getHostName());
        metrics.setTag("chunk", subQuery.getFileName());
        debugInfo.runningPosition = "breakpoint 1";
            FileSystemHandler fileSystemHandler = null;
        try {
//            System.out.println(String.format("chunk name %s is being executed !!!", subQuery.getFileName()));


            Long queryId = subQuery.getQueryId();
            TKey leftKey = (TKey) subQuery.getLeftKey();
            TKey rightKey = (TKey) subQuery.getRightKey();
            String fileName = subQuery.getFileName();
            Long timestampLowerBound = subQuery.getStartTimestamp();
            Long timestampUpperBound = subQuery.getEndTimestamp();



//        metrics.setFileName(fileName);

            long start = System.currentTimeMillis();

            metrics.startEvent("total-time");
            metrics.startEvent("create handle");
            debugInfo.runningPosition = "breakpoint 2";
            if (config.HDFSFlag) {
                if (config.HybridStorage && new File(config.dataDir, fileName).exists()) {
                    fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
                    System.out.println("Subquery will be conducted on local file in cache.");
                    metrics.setTag("file system", "local");
                } else {
                    if (config.HybridStorage)
                        System.out.println("Failed to find local file :" + config.dataDir + "/" + fileName);
                    fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
                    metrics.setTag("file system", "HDFS");
                }

            } else {
                fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
                metrics.setTag("file system", "local");
            }
            metrics.endEvent("create handle");
            debugInfo.runningPosition = "breakpoint 3";
            metrics.startEvent("open-file");
            fileSystemHandler.openFile("/", fileName);
            metrics.endEvent("open-file");
            debugInfo.runningPosition = "breakpoint 4";
//        fileTime += (System.currentTimeMillis() - fileStart);

//        long readTemplateStart = System.currentTimeMillis();
            metrics.startEvent("read template");
            Pair data = getTemplateData(fileSystemHandler, fileName);

//        long temlateRead = System.currentTimeMillis() - readTemplateStart;

//            fileSystemHandler.openFile("/", fileName);
//        fileTime += (System.currentTimeMillis() - fileStart);

//        long readTemplateStart = System.currentTimeMillis();
            BTree template = (BTree) data.getKey();
            Integer length = (Integer) data.getValue();



            BTreeLeafNode leaf = null;


//        long searchOffsetsStart = System.currentTimeMillis();
            List<Integer> offsets = template.getOffsetsOfLeafNodesShouldContainKeys(leftKey, rightKey);
            metrics.endEvent("read template");
//        System.out.println("template");
//        System.out.println(offsets);
//        template.printBtree();
//        metrics.setSearchTime(System.currentTimeMillis() - searchOffsetsStart);


//        byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length);
            debugInfo.runningPosition = "breakpoint 5";
//        System.out.println("template");
//        System.out.println(offsets);
//        template.printBtree();
//        metrics.setSearchTime(System.currentTimeMillis() - searchOffsetsStart);


//        byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length);

            metrics.startEvent("leaf node scanning");
            byte[] bytesToRead = readLeafBytesFromFile(fileSystemHandler, offsets, length);
            debugInfo.runningPosition = "breakpoint 6";
//        long leafBytesReadingTime = System.currentTimeMillis() - readLeafBytesStart;
//        metrics.setLeafBytesReadingTime(leafBytesReadingTime);

//        long totalTupleGet = 0L;
//        long totalLeafRead = 0L;
            Input input = new Input(bytesToRead);

            int count = 0;
            debugInfo.runningPosition = "breakpoint 7";
            for (Integer offset : offsets) {
                BlockId blockId = new BlockId(fileName, offset + length + 4);

                long readLeaveStart = System.currentTimeMillis();
                leaf = (BTreeLeafNode) getFromCache(blockId);
                debugInfo.runningPosition = String.format("breakpoint 8.%d.1", count);
                if (leaf == null) {
                    leaf = getLeafFromExternalStorage(input);
                    CacheData cacheData = new LeafNodeCacheData(leaf);
                    putCacheData(blockId, cacheData);
                }
                debugInfo.runningPosition = String.format("breakpoint 8.%d.2", count);
//            totalLeafRead += (System.currentTimeMillis() - readLeaveStart);

                long tupleGetStart = System.currentTimeMillis();
                ArrayList<byte[]> tuplesInKeyRange = leaf.getTuplesWithinKeyRange(leftKey, rightKey);
                debugInfo.runningPosition = String.format("breakpoint 8.%d.3", count);
                //deserialize
                tuplesInKeyRange.stream().forEach(e -> dataTuplesInKeyRange.add(schema.deserializeToDataTuple(e)));
                debugInfo.runningPosition = String.format("breakpoint 8.%d.4", count);
                count++;
            }
            metrics.endEvent("leaf node scanning");
            debugInfo.runningPosition = "breakpoint 9";
//filter by timestamp range
            metrics.startEvent("temporal filter");
            filterByTimestamp(dataTuplesInKeyRange, timestampLowerBound, timestampUpperBound);
            metrics.endEvent("temporal filter");



//            timestampRangeTime += System.currentTimeMillis() - startTime;


            //filter by predicate
//            System.out.println("Before predicates: " + dataTuples.size());
            metrics.startEvent("predicate");
            filterByPredicate(dataTuplesInKeyRange, subQuery.getPredicate());
            metrics.endEvent("predicate");
//            System.out.println("After predicates: " + dataTuples.size());

            debugInfo.runningPosition = "breakpoint 10";

            metrics.startEvent("aggregate");
            if (subQuery.getAggregator() != null) {
                Aggregator.IntermediateResult intermediateResult = subQuery.getAggregator().createIntermediateResult();
                subQuery.getAggregator().aggregate(dataTuplesInKeyRange, intermediateResult);
                dataTuplesInKeyRange.clear();
                dataTuplesInKeyRange.addAll(subQuery.getAggregator().getResults(intermediateResult).dataTuples);
            }
            metrics.endEvent("aggregate");


//            aggregationTime += System.currentTimeMillis() - startTime;



            debugInfo.runningPosition = "breakpoint 11";

//            totalTupleGet += (System.currentTimeMillis() - tupleGetStart);


            //serialize
            metrics.startEvent("serialize");
            if (subQuery.getAggregator() != null) {
                DataSchema outputSchema = subQuery.getAggregator().getOutputDataSchema();
                dataTuplesInKeyRange.stream().forEach(p -> serializedDataTuples.add(outputSchema.serializeTuple(p)));
            } else {fileSystemHandler.closeFile();
                dataTuplesInKeyRange.stream().forEach(p -> serializedDataTuples.add(schema.serializeTuple(p)));
            }
            metrics.endEvent("serialize");

//            tuples.addAll(serializedDataTuples);



            metrics.endEvent("total-time");

//        metrics.setFileReadingTime(fileReadingTime);
//        metrics.setKeyRangTime(keyRangeTime);
//        metrics.setTimestampRangeTime(timestampRangeTime);
//        metrics.setPredicationTime(predicationTime);
//        metrics.setAggregationTime(aggregationTime);

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

//        metrics.setFileReadingTime(fileReadingTime);
//        metrics.setKeyRangTime(keyRangeTime);
//        metrics.setTimestampRangeTime(timestampRangeTime);
//        metrics.setPredicationTime(predicationTime);
//        metrics.setAggregationTime(aggregationTime);

            debugInfo.runningPosition = "breakpoint 12";
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

//        tuples.clear();
//        System.out.println(String.format("%d tuples are found on file %s", tuples.size(), fileName));
//            System.out.println(String.format("chunk name %s has been finished !!!", subQuery.getFileName()));

        } finally {
            if (fileSystemHandler != null)
                fileSystemHandler.closeFile();

            collector.emit(Streams.FileSystemQueryStream, new Values(subQuery, serializedDataTuples, metrics));

            if (!config.SHUFFLE_GROUPING_FLAG) {
                collector.emit(Streams.FileSubQueryFinishStream, new Values("finished"));
            }
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


    private BTreeLeafNode getLeafFromExternalStorage(Input input)
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


    private byte[] readLeafBytesFromFile(FileSystemHandler fileSystemHandler, List<Integer> offsets, int length) throws IOException {
        Integer endOffset = offsets.get(offsets.size() - 1);
        Integer startOffset = offsets.get(0);

        byte[] bytesToRead = new byte[4];


        fileSystemHandler.readBytesFromFile(endOffset + length + 4, bytesToRead);


        Input input = new Input(bytesToRead);
        int lastLeafLength = input.readInt();


        int totalLength = lastLeafLength + (endOffset - offsets.get(0)) + 4;


        bytesToRead = new byte[totalLength];


        fileSystemHandler.readBytesFromFile(startOffset + length + 4, bytesToRead);

        return bytesToRead;
    }
}