package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import indexingTopology.aggregator.Aggregator;
import indexingTopology.bloom.DataChunkBloomFilters;
import indexingTopology.bloom.DataFunnel;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.TrackedDataTuple;
import indexingTopology.filesystem.FileSystemHandler;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.filesystem.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by acelzj on 1/3/17.
 */
public class Indexer<DataType extends Number & Comparable<DataType>> extends Observable {

    private ArrayBlockingQueue<DataTuple> pendingQueue;

    private LinkedBlockingQueue<DataTuple> inputQueue;

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

    private ArrayBlockingQueue<TrackedDataTuple> trackedDataTupleQueue;

    private Integer estimatedSize;
    private Integer estimatedDataSize;

    private int tupleLength;

    private int keyLength;

    private Long start;

    private List<String> bloomFilterIndexedColumns;

    private Map<String, BloomFilter> columnToFilter;

    private TopologyConfig config;

    public Indexer(int taskId, LinkedBlockingQueue<DataTuple> inputQueue, DataSchema schema,
                   ArrayBlockingQueue<SubQuery<DataType>> queryPendingQueue, TopologyConfig config) {

        this.config = config;

        pendingQueue = new ArrayBlockingQueue<>(1024);

        queryResultQueue = new ArrayBlockingQueue<>(100);

        informationToUpdatePendingQueue = new ArrayBlockingQueue<>(10);

        trackedDataTupleQueue = new ArrayBlockingQueue<>(100);

        this.inputQueue = inputQueue;

        templateUpdater = new TemplateUpdater(config.BTREE_ORDER, config);

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

        this.bTree = new BTree(config.BTREE_ORDER, config);

        kryo = new Kryo();
        kryo.register(BTree.class, new KryoTemplateSerializer(config));
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

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

        bloomFilterIndexedColumns = new ArrayList<>();

        columnToFilter = new HashMap<>();

        inputProcessingThread = new Thread(new InputProcessingRunnable());

        inputProcessingThread.start();

//        Thread checkCapacityThread = new Thread(new CheckCapacityRunnable());
//        checkCapacityThread.start();

        start = System.currentTimeMillis();

        createIndexingThread();

        createQueryThread();
    }

    public void setBloomFilterIndexedColumns(List<String> columns) {
        if (columns == null) {
            this.bloomFilterIndexedColumns = new ArrayList<>();
        } else {
            this.bloomFilterIndexedColumns = columns;
        }
        inializeBloomFilters();
    }

    private void inializeBloomFilters() {
        for (String column: bloomFilterIndexedColumns) {
            BloomFilter filter;
            if (schema.getDataType(column).type == Integer.class) {
                filter = BloomFilter.create(Funnels.integerFunnel(), 30000);
            } else if (schema.getDataType(column).type == Long.class) {
                filter = BloomFilter.create(Funnels.longFunnel(), 30000);
            } else if (schema.getDataType(column).type == Double.class) {
                filter = BloomFilter.create(DataFunnel.getDoubleFunnel(), 30000);
            } else if (schema.getDataType(column).type == String.class) {
                filter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 30000);
            } else {
                throw new RuntimeException("Invalid data type: " + schema.getDataType(column).type);
            }
            columnToFilter.put(column, filter);
        }
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

    public void close() {
        inputProcessingThread.interrupt();

        for (Thread indexingThread : indexingThreads) {
            indexingThread.interrupt();
        }

        for (Thread queryThread : queryThreads) {
            queryThread.interrupt();
        }
    }


    class InputProcessingRunnable implements Runnable {

        @Override
        public void run() {

            ArrayList<DataTuple> drainer = new ArrayList<>();

//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {

//                if (chunkId > 0 && estimatedDataSize >= TopologyConfig.SKEWNESS_DETECTION_THRESHOLD * TopologyConfig.CHUNK_SIZE) {
//                    if (bTree.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_THRESHOLD) {
//                        terminateIndexingThreads();
//
//                        lock.lock();
//                        bTree = templateUpdater.createTreeWithBulkLoading(bTree);
//                        lock.unlock();
//
//                        estimatedDataSize = 0;
//
//                        createIndexingThread();
//                    }
//                }


                if (estimatedSize >= config.CHUNK_SIZE) {
                    while (!pendingQueue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }


                    terminateIndexingThreads();

                    FileSystemHandler fileSystemHandler = null;

                    writeTreeIntoChunk();

                    try {
                        if (config.HDFSFlag) {
                            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
                        } else {
                            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
                        }
                        fileName = "taskId" + taskId + "chunk" + chunkId;
                        long start = System.currentTimeMillis();
                        System.out.println("Before writing into HDFS ###");
                        fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                        System.out.println(String.format("File %s is written in %d ms. ###", fileName,
                                System.currentTimeMillis() - start));

                        if (config.HybridStorage && config.HDFSFlag) {
                            FileSystemHandler localFileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
                            start = System.currentTimeMillis();
                            localFileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                            System.out.println(String.format("File %s is written to the disk cache in %d ms", fileName,
                                    System.currentTimeMillis() - start));
                            System.out.println(fileName + " is written locally.");
                        }

//                        if (config.HDFSFlag) {
//                            fileSystemHandler = new HdfsFileSystemHandler(config.dataDir, config);
//                        } else {
//                            fileSystemHandler = new LocalFileSystemHandler(config.dataDir, config);
//                        }
//                        deserilizeChunkFile(fileSystemHandler, fileName);


                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    DataChunkBloomFilters bloomFilters = new DataChunkBloomFilters(fileName);
                    for (String column: bloomFilterIndexedColumns) {
                        bloomFilters.addBloomFilter(column, columnToFilter.get(column));
                    }


//                    KeyDomain keyDomain = new KeyDomain(minIndexValue, maxIndexValue);
                    keyDomain = new KeyDomain(minIndexValue, maxIndexValue);
//                    TimeDomain timeDomain = new TimeDomain(minTimestamp, maxTimestamp);
                    timeDomain = new TimeDomain(minTimestamp, maxTimestamp);

                    domainToBTreeMapping.put(new Domain(minTimestamp, maxTimestamp, minIndexValue, maxIndexValue), bTree);

                    lock.lock();
                    bTree = bTree.getTemplate();
                    lock.unlock();

                    try {
                        informationToUpdatePendingQueue.put(new FileInformation(fileName, new Domain(keyDomain, timeDomain),
                                numTuples, bloomFilters));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
//                        e.printStackTrace();
                    }

                    setChanged();
                    notifyObservers("information update");

//                    filledBPlusTree.clearPayload();
//                    bTree.clearPayload();
                    executed.set(0L);

                    inializeBloomFilters();

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

                DataTuple firstDataTuple = null;
                try {
                    firstDataTuple = inputQueue.poll(10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
//                    e.printStackTrace();
                }

                if (firstDataTuple == null) {
                    continue;
                }

                drainer.add(firstDataTuple);
                inputQueue.drainTo(drainer, 256);

                for (DataTuple dataTuple: drainer) {
                    try {
                        Long timeStamp = (Long) schema.getValue("timestamp", dataTuple);

                        DataType indexValue = (DataType) schema.getIndexValue(dataTuple);

                        if (indexValue.doubleValue() < minIndexValue) {
                            minIndexValue = indexValue.doubleValue();
                        } else if (indexValue.doubleValue() > maxIndexValue) {
                            maxIndexValue = indexValue.doubleValue();
                        }

//
                        if (timeStamp < minTimestamp) {
                            minTimestamp = timeStamp;
                        } else if (timeStamp > maxTimestamp) {
                            maxTimestamp = timeStamp;
                        }

                        pendingQueue.put(dataTuple);

                        if (dataTuple instanceof TrackedDataTuple) {
                            trackedDataTupleQueue.put((TrackedDataTuple) dataTuple);
                            setChanged();
                            notifyObservers("ack");
                        }

                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                }





                numTuples += drainer.size();

                estimatedSize += (drainer.size() * (keyLength + tupleLength + config.OFFSET_LENGTH));
                estimatedDataSize += (drainer.size() * (keyLength + tupleLength + config.OFFSET_LENGTH));


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
//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {
                try {

                    DataTuple firstDataTuple = null;
                    try {
                        firstDataTuple = pendingQueue.poll(10, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }

                    if (firstDataTuple == null) {
                        if (inputExhausted) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    drainer.add(firstDataTuple);
                    pendingQueue.drainTo(drainer, 256);


                    for (DataTuple tuple : drainer) {
                        localCount++;
                        final DataType indexValue = (DataType) schema.getIndexValue(tuple);
                        final byte[] serializedTuple = schema.serializeTuple(tuple);

                        final DataTuple deserializedDataTuple = schema.deserializeToDataTuple(serializedTuple);


                        bTree.insert((Comparable) indexValue, serializedTuple);

                        // update the bloom filter upon the arrival of a new tuple.
                        for(String column: bloomFilterIndexedColumns) {
                            columnToFilter.get(column).put(schema.getValue(column, tuple));
                        }

                    }

                    executed.addAndGet(drainer.size());

                    drainer.clear();
                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
//                    System.out.println(++count);
                }
//                }
            }
        }
    }



    class QueryRunnable implements Runnable {
        @Override
        public void run() {
//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {

//                try {
//                    processQuerySemaphore.acquire();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

                SubQuery<DataType> subQuery = null;

                try {
                    subQuery = queryPendingQueue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
//                    e.printStackTrace();
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
                    Aggregator.IntermediateResult intermediateResult = subQuery.getAggregator().createIntermediateResult();
                    subQuery.getAggregator().aggregate(dataTuples, intermediateResult);
                    dataTuples = subQuery.getAggregator().getResults(intermediateResult).dataTuples;
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
                    Thread.currentThread().interrupt();
//                    e.printStackTrace();
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

    public TrackedDataTuple getTrackedDataTuple() throws InterruptedException {
        return trackedDataTupleQueue.take();
    }

    private void writeTreeIntoChunk() {

        Output output = new Output(6000000, 500000000);

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
        output.close();
    }


    private void deserilizeChunkFile(FileSystemHandler fileSystemHandler, String fileName) {

        System.out.println("Deserializing chunk name " + fileName);

        fileSystemHandler.openFile("/", fileName);

        byte[] bytesToRead;
        byte[] temlateLengthInBytes = new byte[4];

        fileSystemHandler.readBytesFromFile(0, temlateLengthInBytes);

        Input input = new Input(temlateLengthInBytes);

        int length = input.readInt();

        byte[] templateInBytes = new byte[length];
        fileSystemHandler.readBytesFromFile(4, templateInBytes);

        input = new Input(templateInBytes);
        BTree indexedData = kryo.readObject(input, BTree.class);

        List<Integer> offsets = indexedData.getOffsetsOfLeafNodesShouldContainKeys(0
                , 100000);


        int startOffset = offsets.get(0);


        bytesToRead = new byte[4];
        int lastOffset = offsets.get(offsets.size() - 1);


        fileSystemHandler.readBytesFromFile(lastOffset + length + 4, bytesToRead);

        Input input1 = new Input(bytesToRead);
        int tempLength = input1.readInt();
        int totalLength = tempLength + (lastOffset - offsets.get(0));


        List<BTreeLeafNode> leaves = new ArrayList<>();

        bytesToRead = new byte[totalLength + 4];

//            fileSystemHandler.seek(startOffset + length + 4);
        fileSystemHandler.readBytesFromFile(startOffset + length + 4, bytesToRead);
//            System.out.println("leaf bytes read " + (System.currentTimeMillis() - leafReadStart));

        Input input2 = new Input(bytesToRead);


        for (Integer offset : offsets) {
            input2.setPosition(input2.position() + 4);


            BTreeLeafNode leafNode = kryo.readObject(input2, BTreeLeafNode.class);


            List<byte[]> tuplesInKeyRange = leafNode.getTuplesWithinKeyRange(Integer.MIN_VALUE, Integer.MAX_VALUE);

//                System.out.println(offset);

            for (int i = 0 ; i < tuplesInKeyRange.size(); ++i) {
                try {
                    schema.deserializeToDataTuple(tuplesInKeyRange.get(i));
                } catch (KryoException e) {
                    System.out.println("Deserialization error, the template is ");
                    bTree.printBtree();
                }
            }


        }

        System.out.println("Deserialization for " + fileName + " has been finished!!!");
    }
}
