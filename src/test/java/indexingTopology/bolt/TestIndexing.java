package indexingTopology.bolt;

import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import javafx.util.Pair;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 7/2/17.
 */
public class TestIndexing {
    private ArrayBlockingQueue<Pair> inputQueue;

    private BTree indexedData;

    private TemplateUpdater templateUpdater;


    //    CopyOnWriteArrayList<Long> timer = new CopyOnWriteArrayList<Long>();
    private Long total;
    private final int btreeOrder;
    private int numTuples;
    private int chunkId;
    private double indexValue;
    private int numTuplesBeforeWritting;
    private AtomicInteger numberOfQueries;
    private Long totalTime;
//    private ReentrantLock lock;


    private MemChunk chunk;


    private int count;

    private List<String> fieldNames;
    private List<Class> valueTypes;

    private List<Double> values;

    private AtomicLong executedInChekingThread;

    private int numberOfChunks = 20;

    private IndexingRunnable indexingRunnable;
    private boolean templateMode = false;
    private int numberOfIndexingThreads = 4;

    private List<Thread> indexingThreads = new ArrayList<Thread>();

    private Long totalQueryTime;

    private QueryRunnable queryRunnable;
    private int numberOfQueryThreads;
    private List<Thread> queryThreads = new ArrayList<Thread>();

    private Thread checkingSkewnessThread;

    private Semaphore chuckFilled = new Semaphore(0);

    public TestIndexing(int btreeOrder, int numberOfIndexingThreads, int numberOfQueryThreads, boolean templateMode) {

        this.numberOfQueryThreads = numberOfQueryThreads;
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        this.templateMode = templateMode;

        inputQueue = new ArrayBlockingQueue<Pair>(TopologyConfig.PENDING_QUEUE_CAPACITY);

        this.btreeOrder = btreeOrder;
        chunkId = 0;
        total = 0L;
        numTuples = 0;
        numTuplesBeforeWritting = 1;
        count = 0;

        totalTime = 0L;

        totalQueryTime = 0L;

        indexedData = new BTree(btreeOrder);

        numberOfQueries = new AtomicInteger(0);

        fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
        valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

        templateUpdater = new TemplateUpdater(btreeOrder);

        executedInChekingThread = new AtomicLong(0);

        Thread emitThread = new Thread(new Runnable() {
            public void run() {
                final double x1 = 0;
                final double x2 = 1000;
                final double y1 = 0;
                final double y2 = 500;
                final int partitions = 100;

                final int payloadSize = 10;

                int numTuples = 0;

                long timestamp = 0;

                TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
                RandomGenerator randomGenerator = new Well19937c();
                randomGenerator.setSeed(1000);
                KeyGenerator keyGenerator = new ZipfKeyGenerator( 200048, 0.01, randomGenerator);
//                KeyGenerator keyGenerator = new RoundRobinKeyGenerator(TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK);
//                KeyGenerator keyGenerator = new UniformKeyGenerator();

                while (true) {
                    try {
                        Car car = generator.generate();
                        Double key = keyGenerator.generate();
                        List<Object> values = new ArrayList<>();
                        values.add((double) car.id);
                        values.add(key);
                        values.add(new String(new char[payloadSize]));
                        values.add(timestamp);
//
                        byte[] serializedTuples = serializeValues(values);

                        inputQueue.put(new Pair(key, serializedTuples));
                        ++numTuples;
                        ++total;
//                        System.out.println(numTuples);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (numTuples >= TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
//                        System.out.println("A chunk has been full");
//                        System.out.println(inputQueue.size());
                        chuckFilled.release();
                        long start = System.currentTimeMillis();
                        while (!inputQueue.isEmpty()) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.println(System.currentTimeMillis() - start);
                        totalTime += System.currentTimeMillis() - start;
                        try {
                            // synchronizing indexing threads
                            indexingRunnable.setInputExhausted();
                            for (Thread thread : indexingThreads) {
                                thread.join();
                            }
                            indexingThreads.clear();
                            indexingRunnable = new IndexingRunnable();

//                            queryRunnable.terminate();
                            for (Thread thread : queryThreads) {
                                try {
                                    thread.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            queryThreads.clear();
                            queryRunnable = new QueryRunnable();
//                            System.out.println("All the indexing threads are terminated!");

                            numTuples = 0;

                            // synchronizing query threads
//                        queryRunnable.terminate();
//                        for (Thread thread : queryThreads) {
//                            thread.join();
//                        }
//                        queryThreads.clear();
//                        queryRunnable = new QueryRunnable();
//                        System.out.println("All the query threads are terminated!");


//                            indexedData.printStatistics();
                            createEmptyTree();
                            new Thread(new Runnable() {
                                public void run() {
//                                    populateInputQueueWithMoreTuples(5000);
                                    waitForInputQueueFilled();
                                    createIndexingThread();
//                                    createQueryThread();
                                }
                            }).start();

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }


                    }

//                    System.out.println(numberOfQueries.get());

                    if (total >= numberOfChunks * TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                        System.out.println(String.format("Index throughput = %f tuple / s", numberOfChunks * TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK / (double) (totalTime) * 1000));
                        indexingRunnable.setInputExhausted();
                        for (Thread thread : indexingThreads) {
                            try {
                                thread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        indexingThreads.clear();
                        break;
                    }

//                    System.out.println(numberOfQueries.get());

//                    if (numberOfQueries.get() >= 1000) {
//                        System.out.println(String.format("Query throughput = %f ", totalQueryTime / (double) (numberOfQueries.get()) * 1000));
//                        indexingRunnable.setInputExhausted();
//                        for (Thread thread : indexingThreads) {
//                            try {
//                                thread.join();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        indexingThreads.clear();
//                        queryRunnable.terminate();
//                        for (Thread thread : queryThreads) {
//                            try {
//                                thread.join();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        queryThreads.clear();
//                        break;
//                    }

//                    System.out.println(numberOfQueries.get());
                }
            }

        });
        emitThread.start();


        class CheckingSkewnessRunnable implements Runnable {

            @Override
            public void run() {

                ArrayList<Pair> drainer = new ArrayList<>();

                while (true) {


                    if (executedInChekingThread.get() >= TopologyConfig.SKEWNESS_DETECTION_THRESHOLD) {
                        if (indexedData.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_PERCENTAGE) {
                            indexingRunnable.setInputExhausted();
                            for (Thread thread : indexingThreads) {
                                try {
                                    thread.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            indexingThreads.clear();

                            System.out.println("begin to rebuild the template!!!");

                            long start = System.currentTimeMillis();

                            indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);

                            System.out.println("Time used to update template " + (System.currentTimeMillis() - start));

//                        System.out.println("New tree has been built");
//
                            executedInChekingThread.set(0L);
//
                            createIndexingThread();
                        }
                    }
                }
            }
        }



//        populateInputQueueWithMoreTuples(5000);
        waitForInputQueueFilled();

//        System.out.println("Indexing threads have been created!!!");

        createIndexingThread();

        checkingSkewnessThread = new Thread(new CheckingSkewnessRunnable());
        checkingSkewnessThread.start();

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
    }


    class QueryRunnable implements Runnable {

        boolean terminating = false;

        public void terminate() {
            terminating = true;
        }
        public void run() {
            int count = 0;
            while (true) {
//                try {
                    if(terminating) {
                        break;
                    }


//                    Thread.sleep(1);
                    Double leftKey = 0.0;
                    Double rightKey = 0.01;
//                    Double indexValue = random.nextDouble() * 700 + 300;
//                        s2.acquire();
                    long start = System.currentTimeMillis();
                    indexedData.searchRange(leftKey, rightKey);
                    numberOfQueries.incrementAndGet();

//                    bulkLoader.pointSearch(indexValue);



                    long time = System.currentTimeMillis() - start;
//                        indexedData.searchRange(leftKey, rightKey);
//                        s2.release();
//                        indexedData.searchRange(leftKey, rightKey);
                    totalQueryTime += time;

//                    System.out.println(numberOfQueries.get());

//                    if (numberOfQueries.get() % 10000 == 0) {
//                        System.out.println("**********");
//                    }

                    if (numberOfQueries.get() == 5000) {
//                        double aveQueryTime = (double) totalTime.get() / (double) numberOfQueries;
//                        System.out.println(aveQueryTime);
//                        String content = "" + aveQueryTime;
//                        String newline = System.getProperty("line.separator");
//                        byte[] contentInBytes = content.getBytes();
//                        byte[] nextLineInBytes = newline.getBytes();
//                        System.out.println(String.format("%d queries executed!", numberOfQueries));
                        System.out.println("latency per query: " + totalQueryTime / (double)5000 + " ms");
                        totalQueryTime = 0L;
                        numberOfQueries.set(0);
                        break;
//                        totalTime = new AtomicLong(0);
                    }
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    break;
//                }
            }
//            System.out.println(String.format("Query thread %d is terminated!", Thread.currentThread().getId()));
//            System.out.println(String.format("%2.4f ms per query.", totalTime.get() / (double) numberOfQueries / 1000000));
        }
    }


    public byte[] serializeValues(List<Object> values) throws IOException {
        Output output = new Output(1000, 2000000);
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                output.writeDouble((Double) values.get(i));
            } else if (valueTypes.get(i).equals(String.class)) {
                output.writeString((String) values.get(i));
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
        output.writeLong((Long) values.get(valueTypes.size()));
        return output.toBytes();
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
        if(!templateMode || indexedData == null) {
            indexedData = new BTree(btreeOrder);
        } else {
            indexedData.clearPayload();
        }
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
//                        if(!first)
//                            Thread.sleep(100);
                    if(inputExhausted)
                        break;

                    inputQueue.drainTo(drainer,256);
//                        Pair pair = inputQueue.poll(10, TimeUnit.MILLISECONDS);

                    if(drainer.size() == 0) {
                        if(inputExhausted)
                            break;
                        else
                            continue;
                    }
                    for(Pair pair: drainer) {
                        localCount++;
                        final Double indexValue = (Double) pair.getKey();
                        final byte[] serializedTuple = (byte[]) pair.getValue();
                        indexedData.insert(indexValue, serializedTuple);
                    }
                    executed.addAndGet(drainer.size());
                    executedInChekingThread.addAndGet(drainer.size());
//                    total.addAndGet(drainer.size());
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
//                        if (!inputQueue.isEmpty()) {
//                            try {
//                                Pair pair = inputQueue.take();
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

    private void waitForInputQueueFilled() {
        try {
            chuckFilled.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int bTreeOrder = 64;
        TestIndexing test = new TestIndexing(bTreeOrder, 1, 0, true);
    }
}
