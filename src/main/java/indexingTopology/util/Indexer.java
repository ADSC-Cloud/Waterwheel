package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by acelzj on 1/3/17.
 */
public class Indexer<DataType extends Number & Comparable<DataType>> extends Observable {

    private ArrayBlockingQueue<DataTuple> pendingQueue;

    private ArrayBlockingQueue<DataTuple> inputQueue;

    private ArrayBlockingQueue<SubQuery<DataType>> queryPendingQueue;

    private Map<Domain, BTree> domainToBTreeMapping;

    private BTree bTree;

    private IndexingRunnable indexingRunnable;

    private QueryRunnable queryRunnable;

    private List<Thread> indexingThreads;

    private List<Thread> queryThreads;

    private TemplateUpdater templateUpdater;

    private Thread inputProcessingThread;

    private final static int numberOfIndexingThreads = 1;

    private final static int numberOfQueryThreads = 1;

    private AtomicLong executed;

    private Long numTuples;

    private MemChunk chunk;

    private int chunkId;

    private String indexField;

    private Kryo kryo;

    private Double minIndexValue;
    private Double maxIndexValue;

    private Long minTimestamp;
    private Long maxTimestamp;

    private DataSchema schema;

    private int taskId;

//    private Semaphore processQuerySemaphore;
    private Lock lock;

    private TimeDomain timeDomain;

    private KeyDomain keyDomain;

    private String fileName;

    private ArrayBlockingQueue<Pair> queryResultQueue;

    private ArrayBlockingQueue<FileInformation> informationToUpdatePendingQueue;

    private Integer estimatedSize;
    private Integer estimatedDataSize;

    private int tupleLength;

    private int keyLength;

    private Long start;

    public Indexer(int taskId, ArrayBlockingQueue<DataTuple> inputQueue, DataSchema schema, ArrayBlockingQueue<SubQuery<DataType>> queryPendingQueue) {
        pendingQueue = new ArrayBlockingQueue<>(1024);

        queryResultQueue = new ArrayBlockingQueue<Pair>(100);

        informationToUpdatePendingQueue = new ArrayBlockingQueue<FileInformation>(10);

        this.inputQueue = inputQueue;

        templateUpdater = new TemplateUpdater(TopologyConfig.BTREE_ORDER);

        start = System.currentTimeMillis();

        executed = new AtomicLong(0);

        numTuples = 0L;

        chunkId = 0;

        indexingThreads = new ArrayList<>();
        queryThreads = new ArrayList<>();

        this.indexField = schema.getIndexField();

        this.schema = schema.duplicate();

//        this.processQuerySemaphore = new Semaphore(1);
        this.lock = new ReentrantLock();

        this.bTree = new BTree(TopologyConfig.BTREE_ORDER);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        tupleLength = schema.getTupleLength();
        keyLength = schema.getIndexType().length;

        this.taskId = taskId;

        this.estimatedSize = 0;
        this.estimatedDataSize = 0;

        this.queryPendingQueue = queryPendingQueue;

        this.domainToBTreeMapping = new HashMap<>();

        minIndexValue = Double.MAX_VALUE;
        maxIndexValue = Double.MIN_VALUE;

        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;

        inputProcessingThread = new Thread(new InputProcessingRunnable());

        inputProcessingThread.start();

//        Thread checkCapacityThread = new Thread(new CheckCapacityRunnable());
//        checkCapacityThread.start();

        start = System.currentTimeMillis();

        createIndexingThread();

        createQueryThread();
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
//            System.out.println("query thread has been created!!!");
            queryThreads.add(queryThread);
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
        createIndexingThread(numberOfIndexingThreads);
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


    class InputProcessingRunnable implements Runnable {

        @Override
        public void run() {

            ArrayList<DataTuple> drainer = new ArrayList<>();

            while (true) {

                if (estimatedDataSize >= TopologyConfig.SKEWNESS_DETECTION_THRESHOLD * TopologyConfig.CHUNK_SIZE) {
                    if (bTree.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_THRESHOLD) {
//                        while (!pendingQueue.isEmpty()) {
//                            try {
//                                Thread.sleep(1);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
                        terminateIndexingThreads();
//
                        lock.lock();

//                        System.out.println("begin to rebuild the template!!!");
//
//                        long start = System.currentTimeMillis();
//
                        bTree = templateUpdater.createTreeWithBulkLoading(bTree);
//
                        lock.unlock();

//                        System.out.println("Time used to update template " + (System.currentTimeMillis() - start));
//
//                        System.out.println("New tree has been built");

                        estimatedDataSize = 0;

                        createIndexingThread();
                    }
                }
//                System.out.println("size " + estimatedSize);
//                System.out.println("tuple " + numTuples);


//                if (numTuples >= TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                if (estimatedSize >= TopologyConfig.CHUNK_SIZE) {
                    while (!pendingQueue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

//                    System.out.println("A chunk full " + (System.currentTimeMillis() - start)*1.0 / 1000);

//                    System.out.println("Throughput " + executed.get() * 1000 / ((System.currentTimeMillis() - start)*1.0));

                    terminateIndexingThreads();

                    FileSystemHandler fileSystemHandler = null;


                    writeTreeIntoChunk();

                    try {
                        if (TopologyConfig.HDFSFlag) {
                            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
                        } else {
                            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
                        }
                        fileName = "taskId" + taskId + "chunk" + chunkId;
                        fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

//                    KeyDomain keyDomain = new KeyDomain(minIndexValue, maxIndexValue);
                    keyDomain = new KeyDomain(minIndexValue, maxIndexValue);
//                    TimeDomain timeDomain = new TimeDomain(minTimestamp, maxTimestamp);
                    timeDomain = new TimeDomain(minTimestamp, maxTimestamp);

//                    domainToBTreeMapping.put(new Domain(minTimestamp, maxTimestamp, minIndexValue, maxIndexValue), bTree);
                    domainToBTreeMapping.put(new Domain(minTimestamp, maxTimestamp, minIndexValue, maxIndexValue), bTree);

//                    bTree = bTree.getTemplate();
                    lock.lock();
                    bTree = bTree.getTemplate();
                    lock.unlock();
//                    if (filledBPlusTree == null) {
//                        System.out.println("cloned Indexed data null");
//                        System.exit(120);
//                    }

                    try {
                        informationToUpdatePendingQueue.put(new FileInformation(fileName, new Domain(keyDomain, timeDomain), numTuples));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    setChanged();
                    notifyObservers("information update");

//                    filledBPlusTree.clearPayload();
//                    bTree.clearPayload();
                    executed.set(0L);

                    minIndexValue = Double.MAX_VALUE;
                    maxIndexValue = Double.MIN_VALUE;

                    minTimestamp = Long.MAX_VALUE;
                    maxTimestamp = Long.MIN_VALUE;

                    createIndexingThread();

                    start = System.currentTimeMillis();

                    numTuples = 0L;

                    estimatedSize = 0;

                    ++chunkId;
                }

                inputQueue.drainTo(drainer, 256);


                for (DataTuple dataTuple: drainer) {
                    try {
//                        Double indexValue = tuple.getDoubleByField(indexField);
                        Long timeStamp = (Long) schema.getValue("timestamp", dataTuple);

                        DataType indexValue = (DataType) schema.getIndexValue(dataTuple);
                        if (indexValue.doubleValue() < minIndexValue) {
                            minIndexValue = indexValue.doubleValue();
                        }
                        if (indexValue.doubleValue() > maxIndexValue) {
                            maxIndexValue = indexValue.doubleValue();
                        }
//
                        if (timeStamp < minTimestamp) {
                            minTimestamp = timeStamp;
                        }
                        if (timeStamp > maxTimestamp) {
                            maxTimestamp = timeStamp;
                        }
//                        byte[] serializedTuple = schema.serializeTuple(tuple);
//
//                        Pair pair = new Pair(indexValue, serializedTuple);
//
                        pendingQueue.put(dataTuple);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                numTuples += drainer.size();

                estimatedSize += (drainer.size() * (keyLength + tupleLength + TopologyConfig.OFFSET_LENGTH));
                estimatedDataSize += (drainer.size() * (keyLength + tupleLength + TopologyConfig.OFFSET_LENGTH));

                drainer.clear();

            }
        }
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
            ArrayList<DataTuple> drainer = new ArrayList<>();
            while (true) {
                try {
//                        Pair pair = queue.poll(1, TimeUnit.MILLISECONDS);
//                        if (pair == null) {
//                        if(!first)
//                            Thread.sleep(100);

                    pendingQueue.drainTo(drainer, 256);

//                    System.out.println(String.format("%d executed ", executed.get()));
//                    System.out.println(String.format("%d tuples have been drained to drainer ", drainer.size()));

//                        Pair pair = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (drainer.size() == 0) {
                        if (inputExhausted)
                            break;
                        else
                            continue;
                    }

                    for (DataTuple tuple : drainer) {
                        localCount++;
                        final DataType indexValue = (DataType) schema.getIndexValue(tuple);
                        final byte[] serializedTuple = schema.serializeTuple(tuple);
//                        if (filledBPlusTree != null) {
//                            filledBPlusTree.insert((Comparable) indexValue, serializedTuple);
//                        } else {
//                            if (bTree == null) {
//                                System.exit(111);
//                                System.out.println("indexed data is null");
//                            }
                            bTree.insert((Comparable) indexValue, serializedTuple);
//                        }
                    }

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
                if (inputQueue.size() * 1.0 / TopologyConfig.PENDING_QUEUE_CAPACITY < 0.1) {
                    System.out.println(inputQueue.size() * 1.0 / TopologyConfig.PENDING_QUEUE_CAPACITY);
                    System.out.println("Warning : the production speed is too slow!!!");
                    System.out.println(++count);
                }
//                }
            }
        }
    }



    class QueryRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {

//                try {
//                    processQuerySemaphore.acquire();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

                SubQuery<DataType> subQuery = null;

                try {
                    subQuery = queryPendingQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


                Long queryId = subQuery.getQueryId();
                DataType leftKey = subQuery.getLeftKey();
                DataType rightKey = subQuery.getRightKey();
                Long startTimestamp = subQuery.getStartTimestamp();
                Long endTimestamp = subQuery.getEndTimestamp();

                lock.lock();

                List<byte[]> serializedTuples = new ArrayList<>();
                serializedTuples.addAll(bTree.searchRange(leftKey, rightKey));

                List<BTree> bTrees = new ArrayList<>(domainToBTreeMapping.values());
                for (BTree bTree : bTrees) {
                    serializedTuples.addAll(bTree.searchRange(leftKey, rightKey));
                }
                lock.unlock();

                List<DataTuple> dataTuples = new ArrayList<>();


                for (int i = 0; i < serializedTuples.size(); ++i) {
                    DataTuple dataTuple = schema.deserializeToDataTuple(serializedTuples.get(i));
                    Long timestamp = (Long) schema.getValue("timestamp", dataTuple);
                    if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
                        if (subQuery.getPredicate() == null || subQuery.getPredicate().test(dataTuple)) {
                            dataTuples.add(dataTuple);
                        }
                    }
                }

                if (subQuery.getAggregator() != null) {
                    subQuery.getAggregator().aggregate(dataTuples);
                    dataTuples = subQuery.getAggregator().getResults().dataTuples;
                }

                List<byte[]> serializedQueryResults = new ArrayList<>();
                for(DataTuple dataTuple: dataTuples) {
                    if (subQuery.getAggregator() != null) {
                        serializedQueryResults.add(subQuery.getAggregator().getOutputDataSchema().serializeTuple(dataTuple));
                    }
                    else {
                        serializedQueryResults.add(schema.serializeTuple(dataTuple));
                    }
                }

//                processQuerySemaphore.release();

                try {
                    queryResultQueue.put(new Pair(subQuery, serializedQueryResults));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                setChanged();
                notifyObservers("query result");
            }
        }
    }

    public void cleanTree(Domain domain) {
//        System.out.println("a tree has been removed!!!");
        lock.lock();
        domainToBTreeMapping.remove(domain);
        lock.unlock();
    }


    public FileInformation getFileInformation() {
        FileInformation fileInformation = null;
        try {
            fileInformation = informationToUpdatePendingQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return fileInformation;
    }

    public Pair getQueryResult() {
        Pair pair = null;
        try {
            pair =  queryResultQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return pair;
    }

    private void writeTreeIntoChunk() {

        Output output = new Output(60000000, 500000000);

        byte[] leafBytesToWrite = bTree.serializeLeaves();

        kryo.writeObject(output, bTree);
        byte[] templateBytesToWrite = output.toBytes();


        output = new Output(4);
        int templateLength = templateBytesToWrite.length;
        output.writeInt(templateLength);

        byte[] templateLengthBytesToWrite = output.toBytes();

        chunk = MemChunk.createNew(leafBytesToWrite.length + 4 + templateLength);
        chunk.write(templateLengthBytesToWrite);
        chunk.write(templateBytesToWrite);
        chunk.write(leafBytesToWrite);
    }
}
