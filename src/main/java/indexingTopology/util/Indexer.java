package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.streams.Streams;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by acelzj on 1/3/17.
 */
public class Indexer {

    private ArrayBlockingQueue<Pair> pendingQueue;

    private ArrayBlockingQueue<Tuple> inputQueue;

    private ArrayBlockingQueue<Pair> queryPendingQueue;

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

    private Long minTimeStamp = Long.MAX_VALUE;
    private Long maxTimeStamp = Long.MIN_VALUE;

    private DataSchema schema;

    private OutputCollector collector;

    private int taskId;

    private Semaphore processQuerySemaphore;

    private BTree clonedIndexedData;

    public Indexer(int taskId, ArrayBlockingQueue<Tuple> inputQueue, String indexedField, DataSchema schema, OutputCollector collector, ArrayBlockingQueue<Pair> queryPendingQueue) {
        pendingQueue = new ArrayBlockingQueue<>(1024);

        this.inputQueue = inputQueue;

        templateUpdater = new TemplateUpdater(TopologyConfig.BTREE_OREDER);

        start = System.currentTimeMillis();

        executed = new AtomicLong(0);

        numTuples = 0;

        chunkId = 0;

        indexingThreads = new ArrayList<>();
        queryThreads = new ArrayList<>();

        this.chunk = chunk;

        this.indexField = indexedField;

        this.schema = schema;

        this.processQuerySemaphore = new Semaphore(1);

        this.indexedData = new BTree(TopologyConfig.BTREE_OREDER);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer());
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        this.collector = collector;

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

            ArrayList<Tuple> drainer = new ArrayList<>();

            while (true) {


                if (executed.get() >= TopologyConfig.NUM_TUPLES_TO_CHECK_TEMPLATE) {
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

                                System.out.println("begin to rebuild the template!!!");

                                long start = System.currentTimeMillis();

                                indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);

                                processQuerySemaphore.release();

                                System.out.println("Time used to update template " + (System.currentTimeMillis() - start));

//                        System.out.println("New tree has been built");
//
                                executed.set(0L);
//
                                createIndexingThread();
                            }
                    }
                }



                if (numTuples >= TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                    while (!pendingQueue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    terminateIndexingThreads();

                    System.out.println(chunkId + " has been full!!!");

                    FileSystemHandler fileSystemHandler = null;
                    String fileName = null;

                    writeTreeIntoChunk();

                    try {
                        if (TopologyConfig.HDFSFlag) {
                            fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
                        } else {
                            fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
                        }
                        fileName = fileName = "taskId" + taskId + "chunk" + chunkId;
                        fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    Pair keyRange = new Pair(minIndexValue, maxIndexValue);
                    Pair timeStampRange = new Pair(minTimeStamp, maxTimeStamp);

                    domainToBTreeMapping.put(new Domain(minTimeStamp, maxTimeStamp, minIndexValue, maxIndexValue), indexedData);

//                    indexedData = indexedData.clone();
                    clonedIndexedData = indexedData.clone();

                    collector.emit(Streams.FileInformationUpdateStream, new Values(fileName, keyRange, timeStampRange));

                    collector.emit(Streams.TimeStampUpdateStream, new Values(timeStampRange, keyRange));

//                    indexedData.clearPayload();
                    clonedIndexedData.clearPayload();

                    executed.set(0L);

                    createIndexingThread();

                    System.out.println(chunkId + " has create new indexing threads!!!");

                    start = System.currentTimeMillis();

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
                inputQueue.drainTo(drainer, 256);

                for (Tuple tuple: drainer) {
                    try {
                        Double indexValue = tuple.getDoubleByField(indexField);
                        Long timeStamp = tuple.getLong(3);
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

                        pendingQueue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
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
            ArrayList<Pair> drainer = new ArrayList<Pair>();
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

                    for (Pair pair : drainer) {
                        localCount++;
                        final Double indexValue = (Double) pair.getKey();
//                            final Integer offset = (Integer) pair.getValue();
                        final byte[] serializedTuple = (byte[]) pair.getValue();
//                            System.out.println("local count " + localCount + " insert");
                        if (clonedIndexedData != null) {
                            clonedIndexedData.insert(indexValue, serializedTuple);
                        } else {
                            indexedData.insert(indexValue, serializedTuple);
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

                Pair pair = null;

                try {
                    pair = queryPendingQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


                Long queryId = (Long) pair.getKey();

                System.out.println("semaphore " + queryId + " has been acquired in query runnable!!!");

                System.out.println("query id " + queryId + "in indexer has been taken from query pending queue!!!");

                Pair keyRange = (Pair) pair.getValue();

                Double leftKey = (Double) keyRange.getKey();

                Double rightKey = (Double) keyRange.getValue();

                List<byte[]> serializedTuples = null;

                serializedTuples = indexedData.searchRange(leftKey, rightKey);

                processQuerySemaphore.release();

                System.out.println("semaphore " + queryId + " has been released in query runnable!!!");

                collector.emit(Streams.BPlusTreeQueryStream, new Values(queryId, serializedTuples));

                System.out.println("query id " + queryId + "in indexer has been finished!!!");
            }
        }
    }

    public void cleanTree(Domain domain) {
        System.out.println("a tree has been removed!!!");
        indexedData = clonedIndexedData;
        clonedIndexedData = null;
        domainToBTreeMapping.remove(domain);
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
