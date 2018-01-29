package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.common.Domain;
import indexingTopology.common.MemChunk;
import indexingTopology.util.generator.KeyGenerator;
import indexingTopology.util.generator.UniformKeyGenerator;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.metric.internal.RateTracker;
import org.apache.storm.task.OutputCollector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by acelzj on 1/3/17.
 */
public class IndexerCopy {

    private ArrayBlockingQueue<Pair> pendingQueue;

    private ArrayBlockingQueue<Pair> inputQueue;

    private BTree indexedData;

    private IndexingRunnable indexingRunnable;

    private QueryRunnable queryRunnable;

    private InputProcessingRunnable inputProcessingRunnable;

    private List<Thread> indexingThreads;

    private List<Thread> queryThreads;

    private TemplateUpdater templateUpdater;

    private Thread inputProcessingThread;

    private int numberOfIndexingThreads = 1;

    private AtomicLong executed;

    private int numTuples;

    private MemChunk chunk;

    private int chunkId;

    private long start;

    private String indexField;

    private List<String> fileNames;

    private Kryo kryo;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimestamp;
    private Long maxTimestamp;

    private DataSchema schema;

    private int choice;

    AtomicLong queryId;

    private OutputCollector collector;

    private int taskId;

    private Semaphore processQuerySemaphore;

    private Map<Domain, BTree> domainToBTreeMapping;

    private Logger logger;

    private BufferedWriter bufferedWriter;

    private int order;

    private Long totalTuples;

    private boolean templateMode;

    private int numberOfRebuild;

    private Long totalRebuildTime;

    private Long startTime;

    private RateTracker rateTracker;


    private Long totalTime;

    private AtomicLong totalQueryTime;
    private int numberOfQueryThreads = 2;

    private CheckCapacityRunnable checkCapacityRunnable;

    Thread capacityCheckingThread;

    private TopologyConfig config;

    public IndexerCopy(int taskId, ArrayBlockingQueue<Pair> inputQueue, BTree indexedData, String indexedField,
                       DataSchema schema, BufferedWriter bufferedWriter, int order, boolean templateMode,
                       int numberOfIndexingThreads, TopologyConfig config) {
        this.config = config;
        pendingQueue = new ArrayBlockingQueue<>(config.PENDING_QUEUE_CAPACITY);

        this.inputQueue = inputQueue;

        this.indexedData = indexedData;

        templateUpdater = new TemplateUpdater(config.BTREE_ORDER, config);

        start = System.currentTimeMillis();

        if (inputProcessingRunnable == null) {
            inputProcessingRunnable = new InputProcessingRunnable();
//            System.out.println("new input processing runnable has been created!!!");
            inputProcessingThread = new Thread(inputProcessingRunnable);
        }

        inputProcessingThread.start();

        executed = new AtomicLong(0);

        numTuples = 0;

        chunkId = 0;

        queryId = new AtomicLong(0L);

        indexingThreads = new ArrayList<>();


        this.indexField = indexedField;

        this.schema = schema;

        this.processQuerySemaphore = new Semaphore(1);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

        this.collector = collector;

        this.taskId = taskId;

        domainToBTreeMapping = new HashMap<>();

        this.numberOfIndexingThreads = numberOfIndexingThreads;

        System.out.println(this.numberOfIndexingThreads);

        createIndexingThread();

        this.logger = Logger.getLogger(IndexerCopy.class);

        Thread queryThread = new Thread(new QueryRunnable());

        this.bufferedWriter = bufferedWriter;

        this.order = order;

        this.totalTuples = 0L;

        this.templateMode = templateMode;

        numberOfRebuild = 0;

        totalRebuildTime = 0L;

        startTime = System.currentTimeMillis();

        rateTracker = new RateTracker(30 * 1000, 10);


        totalTime = 0L;

        totalQueryTime = new AtomicLong(0);

        checkCapacityRunnable = new CheckCapacityRunnable();

        fileNames = new CopyOnWriteArrayList<>();

//        capacityCheckingThread = new Thread(checkCapacityRunnable);

//        capacityCheckingThread.start();



        queryThreads = new ArrayList<>();

        createQueryThread();
//        numberOfIndexingThreads = choice;

        queryThread.start();
    }

    public void terminateQueryThreads() {
        try {
            queryRunnable.setInputExhausted();
            for (Thread thread : queryThreads) {
                thread.join();
            }
            queryThreads.clear();
            queryRunnable = new QueryRunnable();
//            System.out.println("All the indexing threads are terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void terminateIndexingThreads() {
        try {
            indexingRunnable.setInputExhausted();
            for (Thread thread : indexingThreads) {
                thread.join();
            }
            indexingThreads.clear();
            indexingRunnable = new IndexingRunnable();
//            System.out.println("All the indexing threads are terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createIndexingThread() {
//        System.out.println("number of indexing threads " + numberOfIndexingThreads);
        createIndexingThread(numberOfIndexingThreads);
    }

    private void createQueryThread() {
        createQueryThread(numberOfQueryThreads);
    }

    private void createQueryThread(int n) {
        if(queryRunnable == null) {
            queryRunnable = new QueryRunnable();
        }
        for(int i = 0; i < n; i++) {
            Thread queryThread = new Thread(queryRunnable);
            queryThread.start();
//            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            queryThreads.add(queryThread);
        }
    }

    private void createIndexingThread(int n) {
        if(indexingRunnable == null) {
            indexingRunnable = new IndexingRunnable();
        }
        for(int i = 0; i < n; i++) {
            Thread indexThread = new Thread(indexingRunnable);
            indexThread.start();
//            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }

    public BTree getIndexedData() {
        return indexedData;
    }

    public void terminateInputProcessingThread() {
        inputProcessingRunnable.setInputExhausted();
        try {
            inputProcessingThread.join();
            inputProcessingRunnable = null;
            checkCapacityRunnable.setInputExhausted();
            System.out.println("set input exhausted");
            checkCapacityRunnable = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        String text = "" + (totalTuples / 120);
//
//        try {
//            bufferedWriter.write(text);
//            bufferedWriter.newLine();
//            bufferedWriter.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        Long duration = (System.currentTimeMillis() - startTime);
//        System.out.println("duration " + duration);
//        System.out.println("total tuples " + totalTuples);
//        System.out.println("counter " + counter.getCount());
//        System.out.println("duplicate rate : " + counter.getCount()*1.0 / totalTuples);
        System.out.println(String.format("BTree order %d, Throughput %f / s", 64, totalTuples*1.0 / 20));

        System.out.println("average query latency " + totalQueryTime.get() * 1.0 / queryId.get());

//        if (numberOfRebuild != 0) {
//            System.out.println("average rebuild time" + (totalRebuildTime / numberOfRebuild));
//        }

        /*
        try {
            String text = "Throughput " + totalTuples / 60;
            bufferedWriter.write(text);
            bufferedWriter.newLine();
            text = "Total time " + totalRebuildTime;
            bufferedWriter.write(text);
            bufferedWriter.newLine();
            if (numberOfRebuild > 0) {
                text = "rebuild count " + numberOfRebuild;
                bufferedWriter.write(text);
                bufferedWriter.newLine();
                text = "rate " + totalRebuildTime * 1.0 / numberOfRebuild;
                bufferedWriter.write(text);
                bufferedWriter.newLine();
                text = "rate " + numberOfRebuild * 1.0 / chunkId;
                bufferedWriter.write(text);
                bufferedWriter.newLine();
            }
            text = "average chunk full " + totalTime / chunkId;
            bufferedWriter.write(text);
            bufferedWriter.newLine();
            bufferedWriter.flush();
            text = "average query latency " + totalQueryTime.get() * 1.0 / queryId.get();
            bufferedWriter.write(text);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
    }


    class InputProcessingRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        @Override
        public void run() {

            ArrayList<Pair> drainer = new ArrayList<>();

            while (true) {

                if (inputExhausted) {
                    terminateIndexingThreads();
                    break;
                }

                /*
                if (chunkId > 0 &&
                        executed.get() >= TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK * TopologyConfig.SKEWNESS_DETECTION_THRESHOLD) {
                    if (indexedData.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_PERCENTAGE) {
                        while (!pendingQueue.isEmpty()) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
//
                        terminateIndexingThreads();
//
                        long start = System.currentTimeMillis();
//

                        try {
                            processQuerySemaphore.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
//
                        indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);
//
                        executed.set(0L);

//
//                        System.out.println("rebuild time " + (System.currentTimeMillis() - start));
                        totalRebuildTime += System.currentTimeMillis() - start;
//
                        processQuerySemaphore.release();
//
                        ++numberOfRebuild;
//

//                        System.out.println("New tree has been built");
                        createIndexingThread();
                    }
                }
                */

                if (numTuples >= 600000) {
                    while (!pendingQueue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    terminateIndexingThreads();
                    terminateQueryThreads();

                    FileSystemHandler fileSystemHandler = null;
                    String fileName = null;


                    writeTreeIntoChunk();
//
                    try {
                        if (config.HDFSFlag) {
                            fileSystemHandler = new HdfsFileSystemHandler(config.dataChunkDir, config);
                        } else {
                            fileSystemHandler = new LocalFileSystemHandler(config.dataChunkDir, config);
                        }
                        fileName =  "taskId" + taskId + "chunk" + chunkId;
                        fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                        fileNames.add(fileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

//                    Pair keyRange = new Pair(minIndexValue, maxIndexValue);
//                    Pair timeStampRange = new Pair(minTimeStamp, maxTimeStamp);

//                    Domain domain = new Domain(maxTimeStamp, maxTimeStamp, minIndexValue, maxIndexValue);

//                    domainToBTreeMapping.put(domain, indexedData);

//                    System.out.println("a chunk has been full");

//                    indexedData = indexedData.getTemplate();
//                    terminateQueryThreads();

                    if (templateMode) {
                        indexedData.clearPayload();
                    } else {
                        createNewTemplate();
                    }

                    createQueryThread();

                    executed.set(0L);

                    createIndexingThread();

//                    domainToBTreeMapping.remove(domain);

//                    System.out.println(String.format("After %d ms, a chunk has been full", System.currentTimeMillis() - start));

                    totalTime += System.currentTimeMillis() - startTime;

                    startTime = System.currentTimeMillis();

                    numTuples = 0;

                    ++chunkId;
                }

//                try {
//                    Pair pair = inputQueue.take();
//
//                    pendingQueue.put(pair);
//
//                    ++numTuples;
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                inputQueue.drainTo(drainer);

//                System.out.println("drainer " + drainer.size());

                for (Pair pair: drainer) {
                    try {
                        pendingQueue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                numTuples += drainer.size();

                drainer.clear();

            }
        }
    }


    class CheckCapacityRunnable implements Runnable {

        int count = 0;
        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }
        @Override
        public void run() {
            while (true) {

                if (inputExhausted) {
                    break;
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                if (inputQueue.size() / (256 * 8 * 4)) {
//                    System.out.println("Warning : the production is too slow!!!");
                if (inputQueue.size() * 1.0 / config.PENDING_QUEUE_CAPACITY < 0.1) {
                    System.out.println(inputQueue.size() * 1.0 / config.PENDING_QUEUE_CAPACITY);
                    System.out.println("Warning : the production speed is too slow!!!");
                    System.out.println(++count);
                }
//                }
            }
        }
    }

    private void createNewTemplate() {
        indexedData = new BTree(order, config);
    }

    class IndexingRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }


        Long startTime;
        AtomicInteger threadIndex = new AtomicInteger(0);

        Object syn = new Object();


        @Override
        public void run() {
            boolean first = false;
            synchronized (syn) {
                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                    first = true;
                }
            }
            long localCount = 0;
            ArrayList<Pair> drainer = new ArrayList<Pair>();
            while (true) {
                try {
//                        Pair pair = queue.poll(1, TimeUnit.MILLISECONDS);
//                        if (pair == null) {
//                        if(!first)
//                            Thread.sleep(100);

                    pendingQueue.drainTo(drainer, 256);
//                    inputQueue.drainTo(drainer, 256);

//                        Pair pair = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (drainer.size() == 0) {
                        if (inputExhausted)
                            break;
                        else
                            continue;
                    }

                    for (Pair pair : drainer) {
                        localCount++;
                        final Double indexValue = (Double) pair.getKey();
//                            final Integer offset = (Integer) pair.getValue();
                        final byte[] serializedTuple = (byte[]) pair.getValue();
//                            System.out.println("insert");
                        indexedData.insert(indexValue, serializedTuple);


//                        logger.info("tuple has been inserted " + (System.currentTimeMillis() / 1000));
//                            indexedData.insert(indexValue, offset);
//                        String text = "" + (System.currentTimeMillis() / 1000);

//                        bufferedWriter.write(text);

//                        bufferedWriter.newLine();

//                        bufferedWriter.flush();

                    }

                    totalTuples += drainer.size();

                    executed.addAndGet(drainer.size());

                    drainer.clear();

                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
//            if(first) {
//                System.out.println(String.format("Index throughput = %f tuple / s", executed.get() / (double) (System.currentTimeMillis() - startTime) * 1000));
//                System.out.println("Thread execution time: " + (System.currentTimeMillis() - startTime) + " ms.");
//            }
//                System.out.println("Indexing thread " + Thread.currentThread().getId() + " is terminated with " + localCount + " tuples processed!");
        }
    }


    class QueryRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        @Override
        public void run() {
//            RandomGenerator randomGenerator = new Well19937c();
//            randomGenerator.setSeed(1000);
            KeyGenerator keyGenerator = new UniformKeyGenerator();
            while (true) {

                if (inputExhausted) {
                    break;
                }

//                Long queryId = (Long) pair.getKey();
//
//                Pair keyRange = (Pair) pair.getValue();
//
//                Double leftKey = (Double) keyRange.getKey();
//
//                Double rightKey = (Double) keyRange.getValue();
//
//                List<byte[]> serializedTuples = null;
//
//                if (leftKey.compareTo(rightKey) == 0) {
//                    serializedTuples = indexedData.searchTuples(leftKey);
//                } else {
//                    serializedTuples = indexedData.search(leftKey, rightKey);
//                }



//                try {
//                    processQuerySemaphore.acquire();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//                double leftKey = 940;
//                double rightKey = 950;
//
//
//                List<byte[]> serializedTuples = null;
//
//                long start = System.currentTimeMillis();
//                serializedTuples = indexedData.searchRange(leftKey, rightKey);
//                System.out.println("query " + queryId + " has been finished!!!");
//                totalQueryTime.addAndGet(System.currentTimeMillis() - start);
                for (String fileName : fileNames) {
                    try {

//                        System.out.println(fileName);

                        FileSystemHandler fileSystemHandler = null;
                        if (config.HDFSFlag) {
                            fileSystemHandler = new HdfsFileSystemHandler(config.dataChunkDir, config);
                        } else {
                            fileSystemHandler = new LocalFileSystemHandler(config.dataChunkDir, config);
                        }

                        fileSystemHandler.openFile("/", fileName);

                        byte[] templateLengthInBytes = new byte[4];
                        fileSystemHandler.readBytesFromFile(0, templateLengthInBytes);

                        Input input = new Input(templateLengthInBytes);
                        int length = input.readInt();

                        byte[] serializedTemplate = new byte[length];

                        fileSystemHandler.readBytesFromFile(4, serializedTemplate);

                        input = new Input(serializedTemplate);
                        BTree template = kryo.readObject(input, BTree.class);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

//                for (int i = 0; i < serializedTuples.size(); ++i) {
//                    Values deserializedTuple = null;
//                    try {
//                        deserializedTuple = schema.deserialize(serializedTuples.get(i));
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println(deserializedTuple);
//                }

//                processQuerySemaphore.release();

//                System.out.println(queryId + " query has been finished!!!");

                queryId.incrementAndGet();
            }
        }
    }


    private void writeTreeIntoChunk() {
        Output output = new Output(65000000, 20000000);

//        indexedData.printBtree();

        byte[] leavesInBytes = indexedData.serializeLeaves();

        kryo.writeObject(output, indexedData);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        BTree bTree = kryo.readObject(input, BTree.class);

        bTree.printBtree();

        int lengthOfTemplate = bytes.length;

//        System.out.println("length " + lengthOfTemplate);

        output.close();

        output = new Output(4);

        output.writeInt(lengthOfTemplate);

        byte[] lengthInBytes = output.toBytes();

        chunk = MemChunk.createNew(leavesInBytes.length + 4 + lengthOfTemplate);

        chunk.write(lengthInBytes );

        chunk.write(bytes);

        chunk.write(leavesInBytes);

        output.close();
    }
}
