package indexingTopology;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import indexingTopology.Config.Config;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private int numberOfQueries;
    private AtomicLong totalTime;

    private BulkLoader bulkLoader;
    private BTree<Double, Integer> copyOfIndexedData;
    private BufferedReader bufferedReader;
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
    private int numberOfQueryThreads = 2;
    private List<Thread> queryThreads = new ArrayList<Thread>();

    private EmitRunnable emitRunnable;
    private Thread emitThread;

    private boolean finshed;

    public TestIndexing() {
        new TestIndexing(4, 0);
    }

    public TestIndexing(final int btreeOrder, final int choiceOfMethod) {


        queue = new LinkedBlockingQueue<Pair>();

        this.btreeOrder = btreeOrder;
        chunkId = 0;
        total = new AtomicLong(0);
        numTuples = 0;
        numTuplesBeforeWritting = 1;
        this.choiceOfMethod = choiceOfMethod;
//        bytesLimit = 650000;
        bytesLimit = 65000000;
        count = 0;
        finshed = false;

        inputFile = new File("/home/dmir/IndexTopology_experiment/NormalDistribution/input_data");
//        outputFile = new File("src/total_time_thread_4");
        if (choiceOfMethod == 0) {
            outputFile = new File("src/total_time_thread_baseline" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
            queryOutputFile = new File("src/total_time_thread_baseline" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
        } else {
            outputFile = new File("src/total_time_thread_our_method" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
            queryOutputFile = new File("src/total_time_thread_our_method" + btreeOrder + "with_indexing_query"
                    + numberOfIndexingThreads + "and" + numberOfQueryThreads);
        }
//        queryOutputFile = new File("src/query_latency_with_rebuild_4");

        chunk = MemChunk.createNew(bytesLimit);
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

        try {
            bufferedReader = new BufferedReader(new FileReader(inputFile));
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
//        createEmitThread();
//        Thread emitThread = new Thread(new Runnable() {
//            public void run() {
//                while (true) {
//                    String text = null;
//                    try {
//                        text = bufferedReader.readLine();
//                        if (text == null) {
//                            try {
//                                if (bufferedReader != null) {
//                                    bufferedReader.close();
//                                }
//                                bufferedReader = new BufferedReader(new FileReader(inputFile));
//                                text = bufferedReader.readLine();
//                            } catch (FileNotFoundException e) {
//                                e.printStackTrace();
//                            }
//                            bufferedReader.close();
//                            terminateIndexingThreads();
//                            terminateQueryThreads();
//                            break;
//                        }
//                        String[] tokens = text.split(" ");
//                        values = getValuesObject(tokens);
//                        indexValue = values.get(0);
//                        ++numTuples;
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//
//                    int offset = 0;
//                    try {
//                        offset = chunk.write(serializeIndexValue(values));
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    if (offset >= 0) {
//                        Pair pair = new Pair(indexValue, offset);
//                        try {
//                            queue.put(pair);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        bulkLoader.addRecord(pair);
//                    } else {
//                        while (!queue.isEmpty()) {
//                            try {
//                                Thread.sleep(1);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        int processedTuples = numTuples - numTuplesBeforeWritting;
//                        double percentage = (double) sm.getCounter() * 100 / (double) processedTuples;
//                        try {
//                            s2.acquire();
//                            s2.acquire();
//                            createNewTree(percentage);
//                            indexedData.clearPayload();


//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } finally {
//                            s2.release();
//                            s2.release();
//                        }
//                        System.out.println(percentage);
//                        copyTree();
//                        createNewTree(percentage);
//                        indexedData.clearPayload();
//                        indexedData.printBtree();
//                        numTuplesBeforeWritting = numTuples;
//                        long totalTime = total.get();
//
//                        double averageTime = ((double) totalTime / ((double) processedTuples));
//
//                        String content = "" + averageTime;
//                        String newline = System.getProperty("line.separator");
//                        byte[] contentInBytes = content.getBytes();
//                        byte[] nextLineInBytes = newline.getBytes();
//                        chunk = MemChunk.createNew(bytesLimit);
//                        try {
//                            fop.write(contentInBytes);
//                            fop.write(nextLineInBytes);
//                            offset = chunk.write(serializeIndexValue(values));
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }


//                        try {
//                            terminateIndexingThreads();
//                            terminateQueryThreads();
    // synchronizing indexing threads
//                            indexingRunnable.setInputExhausted();
//                            for (Thread thread : indexingThreads) {
//                                thread.join();
//                                assert thread.isAlive() == false;
//                            }
//                            System.out.println();
//                            indexingThreads.clear();
//                            indexingRunnable = new IndexingRunnable();
//                            System.out.println("All the indexing threads are terminated!");

    // synchronizing query threads
//                            queryRunnable.terminate();
//                            for (Thread thread : queryThreads) {
//                                thread.join();
//                                assert thread.isAlive() == false;
//                            }
//                            queryThreads.clear();
//                            queryRunnable = new QueryRunnable();
//                            System.out.println("All the query threads are terminated!");


//                            createEmptyTree();
//                            if (choiceOfMethod == 0) {
//                                createEmptyTree();
//                            } else {
//                                createNewTree(percentage);
//                                indexedData.clearPayload();
//                                System.out.println("Payload has been cleared");
//                            }
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

//                        bulkLoader.resetRecord();
//                        Pair pair = new Pair(indexValue, offset);
//                        bulkLoader.addRecord(pair);
//                        sm.resetCounter();
//                        try {
//                            queue.put(pair);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        createIndexingThread();
//                        createQueryThread();
//                        ++chunkId;
//                        System.out.println("In the emit thread, the chunkId is " + chunkId);
//                        tm.reset();
//                        total = new AtomicLong(0);
//                    }
//                }
//                System.out.println("Emit thread is terminated");
//            }
//        });
//        emitThread.start();
//        try {
//            emitThread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

  /*
        Thread queryThread = new Thread(new Runnable() {
            public void run() {
                int count = 0;
                while (true) {
                    try {
                        Thread.sleep(1);
                        Double leftKey = (double) 100;
                        Double rightKey = (double) 200;
                        Double indexValue = random.nextDouble() * 700 + 300;
                        s2.acquire();
                        long start = System.nanoTime();
//                        indexedData.search(indexValue);
                        indexedData.searchRange(leftKey, rightKey);
                        long time = System.nanoTime() - start;
                        s2.release();*/

//        createIndexingThread();

//        createQueryThread();


//        Thread queryThread = new Thread(new Runnable() {
//            public void run() {
//                int count = 0;
//                while (true) {
//                    try {
//                        Thread.sleep(1);
//                        Double leftKey = (double) 100;
//                        Double rightKey = (double) 200;
//                        Double indexValue = random.nextDouble() * 700 + 300;
////                        s2.acquire();
//                        long start = System.nanoTime();
//                        indexedData.search(indexValue);
////                        indexedData.searchRange(leftKey, rightKey);
//                        long time = System.nanoTime() - start;
////                        s2.release();
////                        indexedData.searchRange(leftKey, rightKey);
//                        totalTime.addAndGet(time);
//                        ++numberOfQueries;
//                        if (numberOfQueries == 100) {
//                            double aveQueryTime = (double) totalTime.get() / (double) numberOfQueries;
//                            System.out.println(aveQueryTime);
//                            String content = "" + aveQueryTime;
//                            String newline = System.getProperty("line.separator");
//                            byte[] contentInBytes = content.getBytes();
//                            byte[] nextLineInBytes = newline.getBytes();
//                            queryFileOutput.write(contentInBytes);
//                            queryFileOutput.write(nextLineInBytes);
//                            System.out.println(String.format("%d queries executed!", numberOfQueries));
//                            numberOfQueries = 0;
//                            totalTime = new AtomicLong(0);
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        });
//        queryThread.start();
//}

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
//                            try {
//                                if (bufferedReader != null) {
//                                    bufferedReader.close();
//                                }
//                                bufferedReader = new BufferedReader(new FileReader(inputFile));
//                                text = bufferedReader.readLine();
//                            } catch (FileNotFoundException e) {
//                                e.printStackTrace();
//                            }
                        bufferedReader.close();
                        terminateIndexingThreads();
                        terminateQueryThreads();
                        break;
                    }
                    String[] tokens = text.split(" ");
                    values = getValuesObject(tokens);
                    indexValue = values.get(0);
                    ++numTuples;
                } catch (IOException e) {
                    e.printStackTrace();
                }

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
                    while (!queue.isEmpty()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    int processedTuples = numTuples - numTuplesBeforeWritting;
                    double percentage = (double) sm.getCounter() * 100 / (double) processedTuples;
                    try {
                        s2.acquire();
                        s2.acquire();
//                            createNewTree(percentage);
//                            indexedData.clearPayload();


                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        s2.release();
                        s2.release();
                    }
                    System.out.println(percentage);
//                        copyTree();
//                        createNewTree(percentage);
//                        indexedData.clearPayload();
//                        indexedData.printBtree();
                    numTuplesBeforeWritting = numTuples;
                    long totalTime = total.get();

                    double averageTime = ((double) totalTime / ((double) processedTuples));

                    String content = "" + averageTime;
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


//                        try {
                    terminateIndexingThreads();
                    terminateQueryThreads();
                    // synchronizing indexing threads
//                            indexingRunnable.setInputExhausted();
//                            for (Thread thread : indexingThreads) {
//                                thread.join();
//                                assert thread.isAlive() == false;
//                            }
//                            System.out.println();
//                            indexingThreads.clear();
//                            indexingRunnable = new IndexingRunnable();
//                            System.out.println("All the indexing threads are terminated!");

                    // synchronizing query threads
//                            queryRunnable.terminate();
//                            for (Thread thread : queryThreads) {
//                                thread.join();
//                                assert thread.isAlive() == false;
//                            }
//                            queryThreads.clear();
//                            queryRunnable = new QueryRunnable();
//                            System.out.println("All the query threads are terminated!");


//                            createEmptyTree();
                    if (choiceOfMethod == 0) {
                        createEmptyTree();
                    } else {
                        createNewTree(percentage);
                        indexedData.clearPayload();
                        System.out.println("Payload has been cleared");
                    }
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

                    bulkLoader.resetRecord();
                    Pair pair = new Pair(indexValue, offset);
                    bulkLoader.addRecord(pair);
                    sm.resetCounter();
                    try {
                        queue.put(pair);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    createIndexingThread();
                    createQueryThread();
                    ++chunkId;
                    System.out.println("In the emit thread, the chunkId is " + chunkId);
//                        tm.reset();
                    total = new AtomicLong(0);
                }
            }
            System.out.println("Emit thread is terminated");
            setIsFinshed();
        }
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
                    Double leftKey = (double) 100;
                    Double rightKey = (double) 200;
                    Double indexValue = random.nextDouble() * 700 + 300;
//                        s2.acquire();
                    long start = System.nanoTime();
//                    indexedData.printBtree();
                    indexedData.search(indexValue);
//                        indexedData.searchRange(leftKey, rightKey);
                    long time = System.nanoTime() - start;
//                        s2.release();
//                        indexedData.searchRange(leftKey, rightKey);
                    totalTime.addAndGet(time);
                    ++numberOfQueries;
                    if (numberOfQueries == 1000) {
                        double aveQueryTime = (double) totalTime.get() / (double) numberOfQueries;
//                        System.out.println(aveQueryTime);
                        String content = "" + aveQueryTime;
                        String newline = System.getProperty("line.separator");
                        byte[] contentInBytes = content.getBytes();
                        byte[] nextLineInBytes = newline.getBytes();
                        queryFileOutput.write(contentInBytes);
                        queryFileOutput.write(nextLineInBytes);
//                        System.out.println(String.format("%d queries executed!", numberOfQueries));
                        numberOfQueries = 0;
                        totalTime = new AtomicLong(0);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(String.format("Query thread %d is terminated!", Thread.currentThread().getId()));
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
        if (percentage > Config.REBUILD_TEMPLATE_PERCENTAGE) {
                System.out.println(Thread.currentThread().getId() + " has been created a new tree");
                System.out.println("New Template has been built");
                indexedData = bulkLoader.createTreeWithBulkLoading();
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
        indexedData = new BTree<Double,Integer>(btreeOrder, tm, sm);
        System.out.println("height of the new tree is " + indexedData.getHeight());
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
            System.out.println(String.format("Query thread %d is created!", thread.getId()));
            queryThreads.add(thread);
        }
    }


    class IndexingRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        AtomicLong executed;
        long startTime;
        public void run() {
            long count = 0;
            startTime = System.currentTimeMillis();
            executed = new AtomicLong(0);
            while (true) {
                    try {
                        Pair pair = queue.poll(1, TimeUnit.MILLISECONDS);
                        if(pair == null) {
                            if(inputExhausted)
                                break;
                            else
                            continue;
                        }
                        Double indexValue = (Double) pair.getKey();
                        Integer offset = (Integer) pair.getValue();
//                            s2.acquire();
                        long start = System.nanoTime();
//                        indexedData.printBtree();
//                            System.out.println("insert");
                        indexedData.insert(indexValue, offset);
                        executed.incrementAndGet();
//                        if(!indexedData.validateParanetReference()) {
//                            System.out.println("Problem is detected!");
//                        }
//                        if(!indexedData.validateNoDuplicatedChildReference()) {
//                            System.out.println("Problem is detected!");
//                        }
                        total.addAndGet(System.nanoTime() - start);
//                            s2.release();
//                        if (count++ % 10000 == 0) {
//                            System.out.println(String.format("%d tuples inserted! by thread %d", count, Thread.currentThread().getId()));
//                        }
//                        if(count == 173 || count == 78 || count == 43) {
//                            System.out.println("");
//                        }
//                        }
                    } catch (UnsupportedGenericException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
            System.out.println("Indexing thread" + Thread.currentThread().getId() + " is terminated!");
            System.out.println(String.format("Index throughput = %f tuple / s", executed.get() / (double)(System.currentTimeMillis() - startTime) * 1000));
        }
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

    private boolean isFinshed() {
        return finshed;
    }

    private void setIsFinshed() {
        finshed = true;
    }


    public static void main(String[] args) throws Throwable {
        int bTreeOder = 4;
        final int NUM_CHOICE_OF_METHODS = 1;
//        int numberOfIndexingThreads = 1;
//        int numberOfQueryThreads = 1;
        for (int i = 0; i < 8; ++i) {
            for (int j = 0; j < NUM_CHOICE_OF_METHODS; ++j) {
                TestIndexing test = new TestIndexing(bTreeOder, j);
                test.createEmitThread();
                test.createIndexingThread();
                test.createQueryThread();
                while (!test.isFinshed()) {
                    Thread.sleep(1);
                }
            }
        }
//        new TestIndexing();
    }
}

