package indexingTopology;

import indexingTopology.Config.TopologyConfig;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import javafx.util.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 9/26/16.
 */
public class TestIndexing {
    private LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
    private BTree<Double, Integer> indexedData;

    private File inputFile;
    private File outputFile;
    private File queryOutputFile;

    private ByteArrayOutputStream bos;

    private SplitCounterModule sm;
    private TimingModule tm;
    private FileOutputStream fop;
    private FileOutputStream queryFileOutput;
    //    CopyOnWriteArrayList<Long> timer = new CopyOnWriteArrayList<Long>();
    private int bytesLimit;
    private int choiceOfMethod;
    private AtomicLong total;
    private int btreeOrder;
    private int numTuples;
    private int chunkId;
    private double indexValue;
    private int numTuplesBeforeWritting;
//    private int numberOfQueries;
    private long numberOfQueries;
    private AtomicLong totalTime;

    private BulkLoader bulkLoader;
    private BTree<Double, Integer> copyOfIndexedData;
    private BufferedReader bufferedReader;
    private BufferedReader bufferedReaderInQueryThread;
    private MemChunk chunk;

    private Random random;

    private int count;

    private List<String> fieldNames;
    private List<Class> valueTypes;

    private List<Double> values;

    private Semaphore s1;
    private Semaphore s2;

    private IndexingRunnable indexingRunnable;
    private int numberOfIndexingThreads = 1;
    private List<Thread> indexingThreads = new ArrayList<Thread>();

    private QueryRunnable queryRunnable;
    private int numberOfQueryThreads = 1;
    private List<Thread> queryThreads = new ArrayList<Thread>();

    private EmitRunnable emitRunnable;
    private Thread emitThread = null;

    private Thread createThread;

    private Semaphore chuckFilled = new Semaphore(0);

    private AtomicLong queryLantency;

    private boolean finished;

    private long startTime;

    private double averageThroughput;
    private double totalThroughput;

    private int numberOfProcessedTuples = 0;

    private long totalBuildTime = 0;

    private boolean treeRebuilt = false;

    private KeyRangeRecorder keyRangeRecorder = new KeyRangeRecorder();

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    public TestIndexing() {
        new TestIndexing(4, 0);
    }

    public TestIndexing(final int btreeOrder, final int choiceOfMethod) {

        queue = new LinkedBlockingQueue<Pair>();
        this.btreeOrder = btreeOrder;
        chunkId = 0;
        total = new AtomicLong(System.nanoTime());
        numTuples = 0;
        numTuplesBeforeWritting = 1;
        this.choiceOfMethod = choiceOfMethod;
//        bytesLimit = 650000;
        bytesLimit = 65000000;
        count = 0;
        finished = false;
        startTime = System.nanoTime();
        totalThroughput = 0;
        numberOfQueries = 0;
        inputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

        if (choiceOfMethod == 0) {
            outputFile = new File("src/test_total_time_thread_baseline" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
            queryOutputFile = new File("src/test_query_baseline" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
        } else {
            outputFile = new File("src/test_total_time_thread_our_method" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
            queryOutputFile = new File("src/test_query_our_method" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
        }

        chunk = MemChunk.createNew(bytesLimit);
        chunk.changeToLeaveNodesStartPosition();

        tm = TimingModule.createNew();
        sm = SplitCounterModule.createNew();

        indexedData = new BTree<Double, Integer>(btreeOrder, tm, sm);
        bulkLoader = new BulkLoader(btreeOrder, tm, sm);

        s2 = new Semaphore(2);

        fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));

        random = new Random(1000);

        totalTime = new AtomicLong(0);

        queryLantency = new AtomicLong(0);

        try {
            bufferedReader = new BufferedReader(new FileReader(inputFile));
            bufferedReaderInQueryThread = new BufferedReader(new FileReader(inputFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (!queryOutputFile.exists()) {
                queryOutputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            queryFileOutput = new FileOutputStream(queryOutputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createEmitThread() {
        emitRunnable = new EmitRunnable();
        emitThread = new Thread(emitRunnable);
        emitThread.start();
    }

    class EmitRunnable implements Runnable {
        public void run() {
            while (true) {
                String text = null;
                try {
                    text = bufferedReader.readLine();
                    if (text == null) {
                        bufferedReader.close();
                        bufferedReader = new BufferedReader(new FileReader(inputFile));
                        text = bufferedReader.readLine();
//                        terminateIndexingThreads();
//                        terminateQueryThreads();
//                        break;

                    }
                    String[] tokens = text.split(" ");
                    values = getValuesObject(tokens);
                    indexValue = values.get(0);

//                    while (indexValue > 500) {
//                        text = bufferedReader.readLine();
//                        tokens = text.split(" ");
//                        values = getValuesObject(tokens);
//                        indexValue = values.get(0);
//                    }
                    ++numTuples;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                byte[] serializedTuple = null;
                try {
                    serializedTuple = serializeIndexValue(values);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (numberOfProcessedTuples < TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                    if (numberOfProcessedTuples == 0) {
                        System.out.println(indexValue);
                    }
                    if (indexValue < minIndexValue) {
                        minIndexValue = indexValue;
                    }
                    if (indexValue > maxIndexValue) {
                        maxIndexValue = indexValue;
                    }
                    Pair pair = new Pair(indexValue, serializedTuple);
                    try {
                        queue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    System.out.println(numberOfProcessedTuples);
                    ++numberOfProcessedTuples;
                } else {
                    System.out.println("A chunk full, number of processed tuples are " + numberOfProcessedTuples);
                    System.out.println("The chunk id is " + chunkId);
//                    System.out.println("A chunk is filled!");
                    chuckFilled.release();
                    startTime = System.nanoTime();
                    while (!queue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    long totalTime = System.nanoTime() - startTime;
                    // synchronizing indexing threads
                    terminateIndexingThreads();
                    terminateQueryThreads();

                    int processedTuples = numTuples - numTuplesBeforeWritting;
                    double percentage = (double) sm.getCounter() * 100 / (double) processedTuples;
                    System.out.println(percentage);
                    numTuplesBeforeWritting = numTuples;

//                    double averageTime = ((double) totalTime / ((double) processedTuples));
                    double throughput = (double) processedTuples / (double) totalTime * 1000000000;
                    String content = "" + throughput;
                    String newline = System.getProperty("line.separator");
                    byte[] contentInBytes = content.getBytes();
                    byte[] nextLineInBytes = newline.getBytes();
                    try {
                        fop.write(contentInBytes);
                        fop.write(nextLineInBytes);
                        serializedTuple = serializeIndexValue(values);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
//                    indexedData.printBtree();
                    indexedData.printStatistics();
//                        createEmptyTree();
                    chunk.changeToLeaveNodesStartPosition();
                    indexedData.writeLeavesIntoChunk(chunk);
                    chunk.changeToStartPosition();
                    byte[] serializedTree = indexedData.serializeTree();
                    chunk.write(serializedTree);

                    FileSystemHandler fileSystemHandler = null;
                    try {
//                        fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                        fileSystemHandler = new HdfsFileSystemHandler("/home/acelzj");
                        fileSystemHandler.writeToFileSystem(chunk, "/", "chunk" + chunkId);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
//
                    keyRangeRecorder.addKeyRangeToFile("chunk" + chunkId, minIndexValue, maxIndexValue);
                    numberOfProcessedTuples = 0;
                    ++chunkId;
//                    System.out.println("In the emit thread, the chunkId is " + chunkId);

                    if (choiceOfMethod == 0) {
                        createEmptyTree();
                    } else {
                        createNewTree(percentage);
                        if (!treeRebuilt) {
                            indexedData.clearPayload();
                        } else {
                            treeRebuilt = false;
                        }
                    }

//                    populateInputQueueWithMoreTuples(5000);

//                    new Thread(new Runnable() {
//                        public void run() {
//                                    populateInputQueueWithMoreTuples(5000);
//                            waitForInputQueueFilled();
//                            createIndexingThread();
//                            createQueryThread();
//                        }
//                    }).start();
                    if (createThread != null) {
                        try {
                            createThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

//                    chunk.changeToStartPosition();
//                    serializedTree = new byte[TopologyConfig.TEMPLATE_SIZE];
//                    chunk.getData().get(serializedTree);
//                    DeserializationHelper deserializationHelper = new DeserializationHelper();
//                    BTree deserializedTree = deserializationHelper.deserializeBTree(serializedTree, 4, new BytesCounter());
//                    System.out.println("The btree is ");
//                    deserializedTree.printBtree();




                    chunk = MemChunk.createNew(bytesLimit);
                    createThread = new Thread(new Runnable() {
                        public void run() {
//                                    populateInputQueueWithMoreTuples(5000);
                            waitForInputQueueFilled();
                            createIndexingThread();
                            createQueryThread();
                        }
                    });
                    createThread.start();

                    bulkLoader.resetRecord();
                    System.out.println("A chunk full " + indexValue);
                    Pair pair = new Pair(indexValue, serializedTuple);
                    bulkLoader.addRecord(pair);
                    sm.resetCounter();
                    try {
                        queue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                        tm.reset();
                    ++numberOfProcessedTuples;
                    total = new AtomicLong(0);
                    maxIndexValue = Double.MIN_VALUE;
                    minIndexValue = Double.MAX_VALUE;
                    if (indexValue > maxIndexValue) {
                        maxIndexValue = indexValue;
                    }
                    if (indexValue < minIndexValue) {
                        minIndexValue = indexValue;
                    }
                }
            }
//            System.out.println("Emit thread is terminated");
//            System.out.println((double) totalBuildTime / (double) chunkId);
//            setIsFinished();
        }



                /*
                int offset = 0;
                try {
                    offset = chunk.write(serializeIndexValue(values));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (offset >= 0) {
                    Pair pair = new Pair(indexValue, offset);
                    try {
                        queue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    bulkLoader.addRecord(pair);
                } else {
                    System.out.println("A chunk is filled!");
                    chuckFilled.release();
                    startTime = System.nanoTime();
                    while (!queue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    long totalTime = System.nanoTime() - startTime;
                    // synchronizing indexing threads
                    terminateIndexingThreads();
                    terminateQueryThreads();

                    int processedTuples = numTuples - numTuplesBeforeWritting;
                    double percentage = (double) sm.getCounter() * 100 / (double) processedTuples;
                    System.out.println(percentage);
                    numTuplesBeforeWritting = numTuples;

//                    double averageTime = ((double) totalTime / ((double) processedTuples));
                    double throughput = (double) processedTuples / (double) totalTime * 1000000000;
                    String content = "" + throughput;
                    String newline = System.getProperty("line.separator");
                    byte[] contentInBytes = content.getBytes();
                    byte[] nextLineInBytes = newline.getBytes();
                    chunk = MemChunk.createNew(bytesLimit);
                    try {
                        fop.write(contentInBytes);
                        fop.write(nextLineInBytes);
                        offset = chunk.write(serializeIndexValue(values));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    indexedData.printStatistics();
//                        createEmptyTree();
                    if (choiceOfMethod == 0) {
                        createEmptyTree();
                    } else {
                        createNewTree(percentage);
                        if (!treeRebuilt) {
                            indexedData.clearPayload();
                            treeRebuilt = false;
                        }
                    }

//                    populateInputQueueWithMoreTuples(5000);

//                    new Thread(new Runnable() {
//                        public void run() {
//                                    populateInputQueueWithMoreTuples(5000);
//                            waitForInputQueueFilled();
//                            createIndexingThread();
//                            createQueryThread();
//                        }
//                    }).start();
                    if (createThread != null) {
                        try {
                            createThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    createThread = new Thread(new Runnable() {
                        public void run() {
//                                    populateInputQueueWithMoreTuples(5000);
                            waitForInputQueueFilled();
                            createIndexingThread();
                            createQueryThread();
                        }
                    });
                    createThread.start();

                    bulkLoader.resetRecord();
                    Pair pair = new Pair(indexValue, offset);
                    bulkLoader.addRecord(pair);
                    sm.resetCounter();
                    try {
                        queue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ++chunkId;
                    System.out.println("In the emit thread, the chunkId is " + chunkId);
//                        tm.reset();
                    total = new AtomicLong(0);
                }
            }
            System.out.println("Emit thread is terminated");
//            try {
//                createThread.join();
//                System.out.println("create thread is terminated");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            averageThroughput = totalThroughput / (double) chunkId;
//            double averageLatency = (double) totalTime.get() / (double) numberOfQueries.get();
//            String throughput = "" + averageThroughput;
//            String latency = "" + averageLatency;
//            System.out.println(throughput);
//            System.out.println(latency);
//            String newline = System.getProperty("line.separator");
//            byte[] throughputInBytes = throughput.getBytes();
//            byte[] nextLineInBytes = newline.getBytes();
//            byte[] latencyInBytes = latency.getBytes();
//            try {
//                fop.write(throughputInBytes);
//                fop.write(nextLineInBytes);
//                queryFileOutput.write(latencyInBytes);
//                queryFileOutput.write(nextLineInBytes);
//                offset = chunk.write(serializeIndexValue(values));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            System.out.println((double) totalBuildTime / (double) chunkId);
            setIsFinished();
        }*/
    }

    class QueryRunnable implements Runnable {

        boolean terminating = false;

        public void terminate() {
            terminating = true;
        }
        public void run() {
            int count = 0;
            while (true) {
                try {
                    if(terminating) {
                        break;
                    }
                    Thread.sleep(1);
//                    Double leftKey = (double) 0;
//                    Double rightKey = (double) 1000;
//                    Double indexValue = random.nextDouble() * 700 + 300;
//                        s2.acquire();
                    long start = System.nanoTime();
//                    indexedData.printBtree();
//                    indexedData.search(indexValue);
//                    indexedData.searchRange(leftKey, rightKey);
//                    System.out.println(indexedData.searchRange(leftKey, rightKey));
//                    System.out.println(indexedData.searchRange(leftKey, rightKey));
//                    indexedData.search(indexValue);

//                    bulkLoader.pointSearch(indexValue);

//                    String text = null;
//                    try {
//                        text = bufferedReaderInQueryThread.readLine();
//                        if (text == null) {
//                            bufferedReaderInQueryThread.close();
//                            bufferedReaderInQueryThread = new BufferedReader(new FileReader(inputFile));
//                            text = bufferedReaderInQueryThread.readLine();
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println(text);
//                    String [] tuple = text.split(" ");

//                    Double key = Double.parseDouble(tuple[0]);
//                    Double key = 499.34001632016685;
//                    while (key > 500) {
//                        try {
//                            text = bufferedReaderInQueryThread.readLine();
//                            tuple = text.split(" ");
//                            key = Double.parseDouble(tuple[0]);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
                    Double key = 493.78181209251983;

//                    List<String> fileNames = keyRangeRecorder.getFileContainingKey(key);
                    String fileName = "chunk0";
//                    System.out.println("The size of fileNames is " + fileNames.size());

//                    System.out.println("The size of file is " + fileNames.size());

//                    for (String fileName : fileNames) {
                        try {
                            System.out.println("The key is " + key);
//                            RandomAccessFile file;
                            System.out.println("File name is " + fileName);
                            FileSystemHandler fileSystemHandler = new HdfsFileSystemHandler("/home/acelzj");
//                            FileSystemHandler fileSystemHandler = new LocalFileSystemHandler("/home/acelzj");
                            fileSystemHandler.openFile("/", fileName);
//                            file = new RandomAccessFile("/home/acelzj/" + fileName, "r");
                            byte[] serializedTree = new byte[TopologyConfig.TEMPLATE_SIZE];
//                            DeserializationHelper deserializationHelper = new DeserializationHelper();
                            BytesCounter counter = new BytesCounter();

                            fileSystemHandler.readBytesFromFile(serializedTree);
//                            file.read(serializedTree, 0, TopologyConfig.TEMPLATE_SIZE);
                            BTree deserializedTree = DeserializationHelper.deserializeBTree(fileSystemHandler, btreeOrder, counter);
                            System.out.println("***********************************************************");
//                            break;
                            /*
                            deserializedTree.printBtree();
                            int offset = deserializedTree.getOffsetOfLeaveNodeShouldContainKey(key);
//                            file.seek(offset);
                            fileSystemHandler.seek(offset);
                            System.out.println("Offset is " + offset);
                            byte[] lengthInBytes = new byte[4];
//                            file.read(lengthInBytes);
                            fileSystemHandler.readBytesFromFile(lengthInBytes);
                            int lengthOfLeaveInBytes = ByteBuffer.wrap(lengthInBytes, 0, 4).getInt();
                            System.out.println("Length of leave in bytes is " + lengthOfLeaveInBytes);
                            byte[] leafInBytes = new byte[lengthOfLeaveInBytes];
//                            file.seek(offset + 4);
//                            file.read(leafInBytes);
                            fileSystemHandler.seek(offset + 4);
                            fileSystemHandler.readBytesFromFile(leafInBytes);
                            BTreeLeafNode deserializedLeaf = deserializationHelper.deserializeLeaf(leafInBytes
                                    , btreeOrder, counter);
//                            System.out.println("The leaf is ");
//                            deserializedLeaf.print();
                            ArrayList<byte[]> serializedTuples = deserializedLeaf.searchAndGetTuples(key);*/
//                            file.close();
                            fileSystemHandler.closeFile();
//                        } catch (FileNotFoundException e) {
//                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
//                    }







/*
                    long time = System.nanoTime() - start;
//                        indexedData.searchRange(leftKey, rightKey);
//                        s2.release();
//                        indexedData.searchRange(leftKey, rightKey);
                    totalTime.addAndGet(time);
                    ++numberOfQueries;
                    if (numberOfQueries == 1000) {
                        double aveQueryTime = (double) totalTime.get() / (double) numberOfQueries;
                        System.out.println(aveQueryTime);
                        String content = "" + aveQueryTime;
                        String newline = System.getProperty("line.separator");
                        byte[] contentInBytes = content.getBytes();
                        byte[] nextLineInBytes = newline.getBytes();
                        queryFileOutput.write(contentInBytes);
                        queryFileOutput.write(nextLineInBytes);
                        System.out.println(String.format("%d queries executed!", numberOfQueries));
                        System.out.println("latency per query: " + aveQueryTime / (double)1000000 + " ms");
                        numberOfQueries = 0;
                        totalTime = new AtomicLong(0);
                    }*/
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
//            System.out.println(String.format("Query thread %d is terminated!", Thread.currentThread().getId()));
//            System.out.println(String.format("%2.4f ms per query.", totalTime.get() / (double) numberOfQueries / 1000000));
        }
    }

    public byte[] serializeIndexValue(List<Double> values) throws IOException{
        bos = new ByteArrayOutputStream();
        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            }
        }
        return bos.toByteArray();
    }

    private void copyTree() throws CloneNotSupportedException {
        if (chunkId == 0) {
            copyOfIndexedData = (BTree) indexedData.clone(indexedData);
        } else {
            indexedData = (BTree) copyOfIndexedData.clone(copyOfIndexedData);
            indexedData.clearPayload();
        }
    }

    private void createNewTree(double percentage) {
        if (percentage > TopologyConfig.REBUILD_TEMPLATE_PERCENTAGE) {
                System.out.println(Thread.currentThread().getId() + " has been created a new tree");
                System.out.println("New Template has been built");
            long startTime = System.currentTimeMillis();
                indexedData = bulkLoader.createTreeWithBulkLoading(indexedData);
            indexedData.setTemplateMode();
//            indexedData = bulkLoader.createTreeWithBulkLoading();
            long duration = System.currentTimeMillis() - startTime;
            System.out.println(String.format("The time used to build the tree is %d ms", duration));
            totalBuildTime += duration;
            treeRebuilt = true;
//            indexedData.printBtree();
        }
    }

    public List<Double> getValuesObject(String [] valuesAsString) throws IOException {

        List<Double> values = new ArrayList<Double>();
        for (int i=0;i < valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                values.add(Double.parseDouble(valuesAsString[i]));
            }
        }
        return values;
    }

    private void createEmptyTree() {
//        if(!templateMode || indexedData == null) {
            indexedData = new BTree<Double, Integer>(btreeOrder, tm, sm, false);
//        } else {
//            indexedData.clearPayload();
//        }
//        System.out.println("height of the new tree is " + indexedData.getHeight());
    }

    private void createIndexingThread() {
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
            Thread thread = new Thread(queryRunnable);
            thread.start();
//            System.out.println(String.format("Query thread %d is created!", thread.getId()));
            queryThreads.add(thread);
        }
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
//                System.out.println(String.format("Index throughput = %f tuple / s", executed.get() / (double) (System.currentTimeMillis() - startTime) * 1000));
//                System.out.println("Thread execution time: " + (System.currentTimeMillis() - startTime) + " ms.");
            }
//                System.out.println("Indexing thread " + Thread.currentThread().getId() + " is terminated with " + localCount + " tuples processed!");
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
//
//
//
//        for(int i = 0; i < n; i++) {
//            Thread indexThread = new Thread(new Runnable() {
//                public void run() {
//                    long count = 0;
//                    while (true) {
//                        if (!queue.isEmpty()) {
//                            try {
//                                Pair pair = queue.take();
//                                Double indexValue = (Double) pair.getKey();
//                                Integer offset = (Integer) pair.getValue();
////                            s2.acquire();
//                                long start = System.nanoTime();
////                            System.out.println("insert");
//                                indexedData.insert(indexValue, offset);
//                                total.addAndGet(System.nanoTime() - start);
////                            s2.release();
//                                if (count++ % 10000 == 0) {
//                                    System.out.println(String.format("%d tuples inserted! by thread %d", count++, Thread.currentThread().getId()));
//                                }
//                            } catch (UnsupportedGenericException e) {
//                                e.printStackTrace();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//                }
//            });
//            indexThread.start();
//        }
    }

    private void populateInputQueueWithMoreTuples(int generationTimeInMillis) {
        try {
            while(generationTimeInMillis > 0) {
                Thread.sleep(1000);
                System.out.println("Waiting for generating a large number of tuples.");
                generationTimeInMillis -= 1000;
            }
        } catch (InterruptedException e) {

        }
    }



    // synchronizing query threads
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


    // synchronizing indexing threads
    public void terminateQueryThreads() {
        try {
            queryRunnable.terminate();
            for (Thread thread : queryThreads) {
                thread.join();
            }
            queryThreads.clear();
            queryRunnable = new QueryRunnable();
            System.out.println("All the query threads are terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitForInputQueueFilled() {
        try {
            chuckFilled.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean isFinished() {
        return finished;
    }

    private void setIsFinished() {
        finished = true;
    }


    public static void main(String[] args) throws Throwable {
        int bTreeOder = 4;
        final int NUM_CHOICE_OF_METHODS = 1;
//        int numberOfIndexingThreads = 1;
//        int numberOfQueryThreads = 1;
        for (int i = 0; i < 1; ++i) {
            for (int j = 0; j < NUM_CHOICE_OF_METHODS; ++j) {
                TestIndexing test = new TestIndexing(bTreeOder, j);
                test.createEmitThread();
                test.waitForInputQueueFilled();
                test.createIndexingThread();
                test.createQueryThread();
                while (!test.isFinished()) {
                    Thread.sleep(1);
                }
                System.out.println("Hello");
            }
            bTreeOder *= 4;
        }
    }
}

