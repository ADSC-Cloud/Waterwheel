package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.DataSchema;
import indexingTopology.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by acelzj on 1/3/17.
 */
public class Indexer<DataType extends Number> extends Observable {

    private ArrayBlockingQueue<DataTuple> pendingQueue;

    private ArrayBlockingQueue<DataTuple> inputQueue;

    private ArrayBlockingQueue<SubQuery<DataType>> queryPendingQueue;

    private Map<Domain, BTree> domainToBTreeMapping;

    private BTree indexedData;

    private IndexingRunnable indexingRunnable;

    private QueryRunnable queryRunnable;

    private List<Thread> indexingThreads;

    private List<Thread> queryThreads;

    private TemplateUpdater templateUpdater;

    private Thread inputProcessingThread;

    private final static int numberOfIndexingThreads = 3;

    private final static int numberOfQueryThreads = 1;

    private AtomicLong executed;

    private int numTuples;

    private MemChunk chunk;

    private int chunkId;

    private long start;

    private String indexField;

    private Kryo kryo;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimestamp = Long.MAX_VALUE;
    private Long maxTimestamp = Long.MIN_VALUE;

    private DataSchema schema;

    private int taskId;

    private Semaphore processQuerySemaphore;

    private BTree clonedIndexedData;

    private TimeDomain timeDomain;

    private KeyDomain keyDomain;

    private String fileName;

    private ArrayBlockingQueue<Pair> queryResultQueue;

    private ArrayBlockingQueue<Pair> updateInformationPendingQueue;

    public Indexer(int taskId, ArrayBlockingQueue<DataTuple> inputQueue, DataSchema schema, ArrayBlockingQueue<SubQuery<DataType>> queryPendingQueue) {
        pendingQueue = new ArrayBlockingQueue<>(1024);

        queryResultQueue = new ArrayBlockingQueue<Pair>(100);

        updateInformationPendingQueue = new ArrayBlockingQueue<Pair>(10);

        this.inputQueue = inputQueue;

        templateUpdater = new TemplateUpdater(TopologyConfig.BTREE_ORDER);

        start = System.currentTimeMillis();

        executed = new AtomicLong(0);

        numTuples = 0;

        chunkId = 0;

        indexingThreads = new ArrayList<>();
        queryThreads = new ArrayList<>();

        this.indexField = schema.getIndexField();

        this.schema = schema;

        this.processQuerySemaphore = new Semaphore(1);

        this.indexedData = new BTree(TopologyConfig.BTREE_ORDER);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        this.taskId = taskId;

        this.queryPendingQueue = queryPendingQueue;

        this.domainToBTreeMapping = new HashMap<>();

        inputProcessingThread = new Thread(new InputProcessingRunnable());

        inputProcessingThread.start();

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
                /*
                if (executed.get() >= TopologyConfig.SKEWNESS_DETECTION_THRESHOLD) {
                    if (indexedData.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_PERCENTAGE) {
                        while (!pendingQueue.isEmpty()) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

//                        System.out.println("pengding queue has been empty, the template can be rebuilt!!!");

//                        terminateIndexingThreads();

//                        System.out.println("indexing threads have been terminated!!!");

//                        try {
//                            System.out.println("trying to acquire the semaphore");
//                            System.out.println("queue length " + processQuerySemaphore.getQueueLength() + "processing runnable");
//                            processQuerySemaphore.acquire();
                            if (processQuerySemaphore.tryAcquire()) {
//                            System.out.println("semaphore has been acquired in input processing runnable!!!");
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
                                terminateIndexingThreads();

//                                System.out.println("begin to rebuild the template!!!");

                                long start = System.currentTimeMillis();

                                indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);

                                processQuerySemaphore.release();

//                                System.out.println("Time used to update template " + (System.currentTimeMillis() - start));

//                        System.out.println("New tree has been built");
//
                                executed.set(0L);
//
                                createIndexingThread();
                            }
                    }
                }
                */


                if (numTuples >= TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                    while (!pendingQueue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    terminateIndexingThreads();

                    FileSystemHandler fileSystemHandler = null;
//                    String fileName = null;
                    fileName = null;

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

                    domainToBTreeMapping.put(new Domain(minTimestamp, maxTimestamp, minIndexValue, maxIndexValue), indexedData);

//                    indexedData = indexedData.clone();
                    clonedIndexedData = indexedData.clone();

                    try {
                        updateInformationPendingQueue.put(new Pair(fileName, new Domain(keyDomain, timeDomain)));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    setChanged();
                    notifyObservers("information update");

//                    collector.emit(Streams.FileInformationUpdateStream, new Values(fileName, keyDomain, timeDomain));

//                    collector.emit(Streams.TimestampUpdateStream, new Values(timeDomain, keyDomain));

//                    indexedData.clearPayload();
                    clonedIndexedData.clearPayload();

                    executed.set(0L);

                    createIndexingThread();

                    start = System.currentTimeMillis();

                    numTuples = 0;

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

                        pendingQueue.put(dataTuple);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }

                numTuples += drainer.size();

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
//                            final Integer offset = (Integer) pair.getValue();
                        final byte[] serializedTuple = schema.serializeTuple(tuple);
//                            System.out.println("local count " + localCount + " insert");
                        if (clonedIndexedData != null) {
                            clonedIndexedData.insert((Comparable) indexValue, serializedTuple);
                        } else {
                            indexedData.insert((Comparable) indexValue, serializedTuple);
                        }

//                        System.out.println("insert has been finished!!!");
//                            indexedData.insert(indexValue, offset);
                    }

                    executed.addAndGet(drainer.size());

//                    if (executed.get() > 500000) {
//                        System.out.println(String.format("%d tuples have been inserted to the tree ", executed.get()));
//                    }

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
        @Override
        public void run() {
            while (true) {

                try {
                    System.out.println("queue length " + processQuerySemaphore.getQueueLength() + "query runnable");
                    processQuerySemaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                SubQuery<DataType> subQuery = null;

                try {
                    subQuery = queryPendingQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


                Long queryId = subQuery.getQueryId();

//                System.out.println("semaphore " + queryId + " has been acquired in query runnable!!!");

//                System.out.println("query id " + queryId + "in indexer has been taken from query pending queue!!!");
                DataType leftKey = subQuery.getLeftKey();
                DataType rightKey = subQuery.getRightKey();
                Long startTimestamp = subQuery.getStartTimestamp();
                Long endTimestamp = subQuery.getEndTimestamp();

                List<byte[]> serializedTuples = new ArrayList<>();
                List<byte[]> serializedTuplesWithinTimestamp = new ArrayList<>();

//                System.out.println(queryId + " search has been started!!!");

                serializedTuples.addAll(indexedData.searchRange((Comparable) leftKey, (Comparable) rightKey));

//                System.out.println(queryId + " search has been finished!!!");

//                System.out.println("tuple size " + serializedTuples.size());

                for (int i = 0; i < serializedTuples.size(); ++i) {
                    DataTuple dataTuple = schema.deserializeToDataTuple(serializedTuples.get(i));
                    Long timestamp = (Long) schema.getValue("timestamp", dataTuple);
                    if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
                        serializedTuplesWithinTimestamp.add(serializedTuples.get(i));
                    }
                }

//                System.out.println("deserialization has been finished!!!");

                processQuerySemaphore.release();

                try {
                    queryResultQueue.put(new Pair(queryId, serializedTuplesWithinTimestamp));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                setChanged();
                notifyObservers("query result");
//                System.out.println("semaphore " + queryId + " has been released in query runnable!!!");

//                collector.emit(Streams.BPlusTreeQueryStream, new Values(queryId, serializedTuplesWithinTimestamp));

//                System.out.println("query id " + queryId + "in indexer has been finished!!!");
            }
        }
    }

    public void cleanTree(Domain domain) {
//        System.out.println("a tree has been removed!!!");
        indexedData = clonedIndexedData;
        clonedIndexedData = null;
        domainToBTreeMapping.remove(domain);
    }


    public Pair getDomainInformation() {
        Pair pair = null;
        try {
            pair = updateInformationPendingQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return pair;
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

        BTree bTree = clonedIndexedData == null ? indexedData : clonedIndexedData;

        Output output = new Output(65000000, 20000000);

        byte[] leavesInBytes = bTree.serializeLeaves();

        kryo.writeObject(output, bTree);

        byte[] bytes = output.toBytes();

        int lengthOfTemplate = bytes.length;

        output = new Output(4);

        output.writeInt(lengthOfTemplate);

        byte[] lengthInBytes = output.toBytes();

        chunk = MemChunk.createNew(leavesInBytes.length + 4 + lengthOfTemplate);

        chunk.write(lengthInBytes );

        chunk.write(bytes);

        chunk.write(leavesInBytes);
    }
}
