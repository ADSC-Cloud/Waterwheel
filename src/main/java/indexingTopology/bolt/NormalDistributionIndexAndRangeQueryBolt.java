package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.NormalDistributionIndexingAndRangeQueryTopology;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import javafx.util.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 11/15/16.
 */
public class NormalDistributionIndexAndRangeQueryBolt extends BaseRichBolt {

    private static final int maxTuples=43842;
    private final static int numberOfIndexingThreads = 1;

    private final DataSchema schema;

    private final int btreeOrder;
    private final int bytesLimit;

    private final String indexField;

    private OutputCollector collector;

    private BTree<Double, Integer> indexedData;
    private BTree<Double, Integer> copyOfIndexedData;

    private FileSystemHandler fileSystemHandler;

    private int numTuples;
    private int numTuplesBeforeWritting;
    private int numWritten;
    private int chunkId;

    private boolean isTreeBuilt;

    private MemChunk chunk;

    private TimingModule tm;
    private SplitCounterModule sm;

    private long processingTime;

    private long totalTime;

    private BulkLoader bulkLoader;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimeStamp = Long.MAX_VALUE;
    private Long maxTimeStamp = Long.MIN_VALUE;

    private File file;
    private File inputFile;
    private File outputFile;

    private FileOutputStream fop;
    private FileOutputStream queryFileOutPut;

    private LinkedBlockingQueue<Pair> queue;

    private List<Thread> indexingThreads = new ArrayList<Thread>();

    private IndexingRunnable indexingRunnable;

    private TopologyContext context;

    public NormalDistributionIndexAndRangeQueryBolt(String indexField, DataSchema schema, int btreeOrder, int bytesLimit) {
        this.schema = schema;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
        this.indexField = indexField;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        this.tm = TimingModule.createNew();
        this.sm = SplitCounterModule.createNew();

        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
        copyOfIndexedData = indexedData;

        chunk = MemChunk.createNew(this.bytesLimit);

        this.numTuples = 0;
        this.numTuplesBeforeWritting = 1;
        this.numWritten = 0;
        this.processingTime = 0;
        this.chunkId = 0;

        this.context = topologyContext;

        this.isTreeBuilt = false;

        this.bulkLoader = new BulkLoader(btreeOrder, tm, sm);

        this.queue = new LinkedBlockingQueue<Pair>(1024);
        this.outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/query_latency_with_nothing_4");
//        this.outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/query_latency_without_rebuild_but_split_256");
//        this.outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/query_latency_with_rebuild_and_split_4");
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            queryFileOutPut = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/specific_time_with_rebuild_and_split_with_query_4_64M");
//        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/specific_time_without_rebuild_but_split_with_query_256_64M");
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/specific_time_with_nothing_4_64M");
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop = new FileOutputStream(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        createIndexingThread();

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.IndexStream)) {
            Double indexValue = tuple.getDoubleByField(indexField);
            Long timeStamp = tuple.getLong(8);
//            Double indexValue = tuple.getDouble(0);
//            System.out.println("The stream is " + NormalDistributionIndexingTopology.IndexStream);
            try {
                if (numTuples < Config.NUMBER_TUPLES_OF_A_CHUNK) {
//                    if (chunkId == 0) {
//                        System.out.println("Num tuples " + numTuples + " " + indexValue);
//                    }
                    if (indexValue < minIndexValue) {
                        minIndexValue = indexValue;
                    }
                    if (indexValue > maxIndexValue) {
                        maxIndexValue = indexValue;
                    }

                    if (timeStamp < minTimeStamp) {
                        minTimeStamp = timeStamp;
                    }
                    if (timeStamp > maxTimeStamp) {
                        maxTimeStamp = timeStamp;
                    }

                    byte[] serializedTuple = schema.serializeTuple(tuple);
                    Pair pair = new Pair(indexValue, serializedTuple);
                    queue.put(pair);

                    ++numTuples;
                } else {

                    while (!queue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    terminateIndexingThreads();

                    double percentage = (double) sm.getCounter() * 100 / (double) numTuples;
//                    indexedData.printBtree();
                    chunk.changeToLeaveNodesStartPosition();
                    indexedData.writeLeavesIntoChunk(chunk);
                    chunk.changeToStartPosition();
//                    byte[] serializedTree = indexedData.serializeTree();
                    byte[] serializedTree = SerializationHelper.serializeTree(indexedData);
                    chunk.write(serializedTree);

//                    createNewTemplate(percentage);
                    indexedData.clearPayload();
//                    if (!isTreeBuilt) {
//                        indexedData.clearPayload();
//                    } else {
//                        isTreeBuilt = false;
//                        indexedData.setTemplateMode();
//                    }

                    FileSystemHandler fileSystemHandler = null;
                    String fileName = null;
                    try {
//                        fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                        fileSystemHandler = new HdfsFileSystemHandler("/home/acelzj");
                        int taskId = context.getThisTaskId();
                        fileName = "taskId" + taskId + "chunk" + chunkId;
                        fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    Pair keyRange = new Pair(minIndexValue, maxIndexValue);
                    Pair timeStampRange = new Pair(minTimeStamp, maxTimeStamp);

                    collector.emit(NormalDistributionIndexingTopology.FileInformationUpdateStream,
                            new Values(fileName, keyRange, timeStampRange));

                    collector.emit(NormalDistributionIndexingAndRangeQueryTopology.TimeStampUpdateStream,
                            new Values(maxTimeStamp));

                    numTuples = 0;

                    chunk = MemChunk.createNew(bytesLimit);
                    sm.resetCounter();
                    byte[] serializedTuple = schema.serializeTuple(tuple);
                    Pair pair = new Pair(indexValue, serializedTuple);
                    queue.put(pair);
                    createIndexingThread();
                    ++numTuples;
                    ++chunkId;

                    minIndexValue = Double.MAX_VALUE;
                    maxIndexValue = Double.MIN_VALUE;
                    if (indexValue < minIndexValue) {
                        minIndexValue = indexValue;
                    }
                    if (indexValue > maxIndexValue) {
                        maxIndexValue = indexValue;
                    }

                    minTimeStamp = Long.MAX_VALUE;
                    maxTimeStamp = Long.MIN_VALUE;
                    if (timeStamp < minTimeStamp) {
                        minTimeStamp = timeStamp;
                    }
                    if (timeStamp > maxTimeStamp) {
                        maxTimeStamp = timeStamp;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
//            System.out.println("The stream is " + NormalDistributionIndexingTopology.BPlusTreeQueryStream);
//            System.out.println("The bolt id is " + context.getThisTaskId());
            Long queryId = tuple.getLong(0);
            Double leftKey = tuple.getDouble(1);
            Double rightKey = tuple.getDouble(2);
//            System.out.println("The key is " + key);
            List<byte[]> serializedTuples = indexedData.searchRange(leftKey, rightKey);
//            System.out.println("The tuple is " + serializedTuples);
//            if (serializedTuples != null) {
                collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                        new Values(queryId, serializedTuples));
//            }
        }
    }

    private void copyTemplate(int chunkId) throws CloneNotSupportedException {
        if (chunkId == 0) {
            copyOfIndexedData = (BTree) indexedData.clone(indexedData);
        } else {
            indexedData = (BTree) copyOfIndexedData.clone(copyOfIndexedData);
        }
    }

    private void createEmptyTree() {
        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
    }


    private void createNewTemplate(double percentage) {
        if (percentage > Config.REBUILD_TEMPLATE_PERCENTAGE) {
            System.out.println("New tree has been built");
            isTreeBuilt = true;
            indexedData = bulkLoader.createTreeWithBulkLoading(indexedData);
        }
    }

    private void debugPrint(int numFailedInsert, Double indexValue) {
        if (numFailedInsert%1000==0) {
            System.out.println("[FAILED_INSERT] : "+indexValue);
            indexedData.printBtree();
        }
    }

/*
    private long buildOneTree(Double indexValue, byte[] serializedTuple) {
        if (numTuples<43842) {
            try {
                indexedData.insert(indexValue, serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }

        else if (numTuples==43842) {
            System.out.println("number of tuples processed : " + numTuples);
            System.out.println("**********************Tree Written***************************");
            indexedDataWoTemplate.printBtree();
            System.out.println("**********************Tree Written***************************");
        }

        return 0;
    }
*/

    private void writeIndexedDataToHDFS() {
        // todo write this to hdfs
        chunk.serializeAndRefresh();
//        try {
//            hdfs.writeToNewFile(indexedData.serializeTree(),"testname"+System.currentTimeMillis()+".dat");
//            System.out.println("**********************************WRITTEN*******************************");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("num_tuples","wo_template_time","template_time","wo_template_written","template_written"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileInformationUpdateStream,
                new Fields("fileName", "keyRange", "timeStampRange"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                new Fields("leftKey", "rightKey", "serializedTuples"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                new Fields("queryId", "serializedTuples"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.TimeStampUpdateStream,
                new Fields("TimeStamp"));
    }

    class IndexingRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        AtomicLong executed;
        Long startTime;
        AtomicInteger threadIndex = new AtomicInteger(0);

        Object syn = new Object();
        public void run() {
            boolean first = false;
            synchronized (syn) {
                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                    first = true;
                }
                if (executed == null)
                    executed = new AtomicLong(0);
            }
            long localCount = 0;
            ArrayList<Pair> drainer = new ArrayList<Pair>();
            while (true) {
                try {
//                        Pair pair = queue.poll(1, TimeUnit.MILLISECONDS);
//                        if (pair == null) {
//                        if(!first)
//                            Thread.sleep(100);
                    queue.drainTo(drainer,256);
//                        Pair pair = queue.poll(10, TimeUnit.MILLISECONDS);
                    if(drainer.size() == 0) {
                        if(inputExhausted)
                            break;
                        else
                            continue;
                    }
                    for(Pair pair: drainer) {
                        localCount++;
                        final Double indexValue = (Double) pair.getKey();
//                            final Integer offset = (Integer) pair.getValue();
                        final byte[] serializedTuple = (byte[]) pair.getValue();
//                            System.out.println("insert");
                        indexedData.insert(indexValue, serializedTuple);
//                            indexedData.insert(indexValue, offset);
                    }
                    executed.getAndAdd(drainer.size());
                    drainer.clear();
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(first) {
                System.out.println(String.format("Index throughput = %f tuple / s", executed.get() / (double) (System.currentTimeMillis() - startTime) * 1000));
                System.out.println("Thread execution time: " + (System.currentTimeMillis() - startTime) + " ms.");
            }
//                System.out.println("Indexing thread " + Thread.currentThread().getId() + " is terminated with " + localCount + " tuples processed!");
        }
    }




    private void createIndexingThread() {
        createIndexingThread(numberOfIndexingThreads);
    }

    private void createIndexingThread(int n) {
        if(indexingRunnable == null) {
            indexingRunnable = new IndexingRunnable();
        }
        for(int i = 0; i < n; i++) {
            Thread indexThread = new Thread(indexingRunnable);
            indexThread.start();
            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }

    public void terminateIndexingThreads() {
        try {
            indexingRunnable.setInputExhausted();
            for (Thread thread : indexingThreads) {
                thread.join();
            }
            indexingThreads.clear();
            indexingRunnable = new IndexingRunnable();
            System.out.println("All the indexing threads are terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
