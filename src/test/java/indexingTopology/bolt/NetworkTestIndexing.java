package indexingTopology.bolt;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.net.InetAddresses;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import indexingTopology.util.MemChunk;
import indexingTopology.util.TemplateUpdater;
import javafx.util.Pair;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by acelzj on 22/3/17.
 */
public class NetworkTestIndexing {
    private ArrayBlockingQueue<Pair> inputQueue;

    private BTree indexedData;

    private TemplateUpdater templateUpdater;


    //    CopyOnWriteArrayList<Long> timer = new CopyOnWriteArrayList<Long>();
    private Long total;
    private final int btreeOrder;
    private int numTuples;

    private double indexValue;
    private AtomicInteger numberOfQueries;
    private Long totalTime;

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

    private int numberOfRebuild;

    private Long totalRebuildTime;

    private AtomicInteger chunkId;

    private BufferedReader bufferedReader;

    private Map<Integer, Integer> countMapping;

    private AtomicInteger estimatedSize;

    private AtomicInteger estimatedDataSize;
    private AtomicInteger totalDataSize;

    File file;

    private int numberOfKeys;
    private Random random;

    private int tupleLength;

    private CheckingSkewnessRunnable checkingSkewnessRunnbale;

    private boolean rebuildable;

    private TopologyConfig config = new TopologyConfig();

    public NetworkTestIndexing(int btreeOrder, int numberOfIndexingThreads, int numberOfQueryThreads, boolean templateMode, boolean rebuildable) {

        this.numberOfQueryThreads = numberOfQueryThreads;
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        this.templateMode = templateMode;
        this.rebuildable = rebuildable;

        inputQueue = new ArrayBlockingQueue<Pair>(config.PENDING_QUEUE_CAPACITY);

        random = new Random(1000);

        this.btreeOrder = btreeOrder;
        chunkId = new AtomicInteger(0);
        total = 0L;
        numTuples = 0;
        count = 0;

        totalTime = 0L;

        totalQueryTime = 0L;

        numberOfRebuild = 0;

        totalRebuildTime = 0L;

        indexedData = new BTree(btreeOrder, config);

        numberOfQueries = new AtomicInteger(0);

        fieldNames = new ArrayList<String>(Arrays.asList("status", "source_ip", "dest_ip", "url", "timestamp"));
        valueTypes = new ArrayList<Class>(Arrays.asList(Integer.class, Integer.class, Integer.class, String.class, Long.class));

//        fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload", "timestamp"));
//        valueTypes = new ArrayList<Class>(Arrays.asList(Long.class, Double.class, String.class, Long.class));

        templateUpdater = new TemplateUpdater(btreeOrder, config);

        executedInChekingThread = new AtomicLong(0);

        file = new File(config.dataFileDir);

        countMapping = new HashMap<>();

        numberOfKeys = 0;

        tupleLength = 20;

        estimatedSize = new AtomicInteger(0);
        estimatedDataSize = new AtomicInteger(0);
        totalDataSize = new AtomicInteger(0);

        Thread emitThread = new Thread(new Runnable() {
            public void run() {
                int numTuples = 0;

                long timestamp = 0;

                String dataPattern = "\\[(\\d+-\\d+-\\d+\\s+\\d+:\\d+:\\d+\\.\\d+)\\]\\s+\\[(-?\\d+)\\]\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(http.*)";

                Pattern r = Pattern.compile(dataPattern);

                while (true) {
                    try {
                        try {
                            bufferedReader = new BufferedReader(new FileReader(file));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }

                        while (true) {
                            String text = null;
                            try {
                                text = bufferedReader.readLine();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            if (text == null) {
                                break;
                            } else {
                                Matcher m = r.matcher(text);

                                if (m.find()) {
                                    int status = Integer.parseInt(m.group(2));

                                    InetAddress inetAddress = InetAddresses.forString(m.group(3));
                                    int sourceIp = InetAddresses.coerceToInteger(inetAddress);

                                    inetAddress = InetAddresses.forString(m.group(4));
                                    int destIp = InetAddresses.coerceToInteger(inetAddress);

                                    String url = m.group(6);

                                    List<Object> values = new ArrayList<>();
                                    values.add(status);
                                    values.add(sourceIp);
                                    values.add(destIp);
                                    values.add(url.substring(0, 21));
                                    values.add(timestamp);

                                    byte[] serializedTuples = serializeValues(values);
                                    inputQueue.put(new Pair(sourceIp, serializedTuples));

                                    if (countMapping.get(destIp) == null) {
                                        countMapping.put(destIp, 1);
                                    } else {
                                        countMapping.put(destIp, countMapping.get(destIp) + 1);
                                    }

                                    estimatedSize.addAndGet(tupleLength + 21);
//                                    estimatedDataSize.addAndGet(tupleLength + url.length());
                                    totalDataSize.addAndGet(tupleLength + 21);
                                }


                                ++timestamp;
                                ++numTuples;
                                ++total;
                            }

//                            System.out.println(estimatedSize);


//                            System.out.println(file.getName());
//                        System.out.println(total);
//                        System.out.println(index);

                            if (estimatedSize.get() >=config.CHUNK_SIZE) {
                                chuckFilled.release();
                                long start = System.currentTimeMillis();
                                while (!inputQueue.isEmpty()) {
                                    try {
//                                        System.out.println(inputQueue.size());
                                        Thread.sleep(1);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                totalTime += System.currentTimeMillis() - start;

                                if (rebuildable) {
                                    checkingSkewnessRunnbale.terminate();
                                    checkingSkewnessThread.join();
                                    checkingSkewnessRunnbale = new CheckingSkewnessRunnable();
                                }

//                                System.out.println("finished!!!");

                                try {
                                    // synchronizing indexing threads
                                    indexingRunnable.setInputExhausted();
                                    for (Thread thread : indexingThreads) {
                                        thread.join();
                                    }
                                    indexingThreads.clear();
                                    indexingRunnable = new IndexingRunnable();

//                                    queryRunnable.terminate();
//                                    for (Thread thread : queryThreads) {
//                                        thread.join();
//                                    }
//                                    queryThreads.clear();
//                                    queryRunnable = new QueryRunnable();

                                    estimatedSize.set(0);
                                    estimatedDataSize.set(0);

                                    numberOfKeys += countMapping.keySet().size();

                                    numTuples = 0;
                                    chunkId.incrementAndGet();

                                    createEmptyTree();
                                    new Thread(new Runnable() {
                                        public void run() {
                                            waitForInputQueueFilled();
                                            createIndexingThread();
                                            if (rebuildable) {
                                                checkingSkewnessThread = new Thread(checkingSkewnessRunnbale);
                                                checkingSkewnessThread.start();
                                            }
//                                    createQueryThread();
                                        }
                                    }).start();

                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }


//                            }
//                        }

//                    System.out.println(numberOfQueries.get());

                                if (totalDataSize.get() >= numberOfChunks * config.CHUNK_SIZE) {
                                    System.out.println(String.format("Index throughput = %f tuple / s", total / (double) (totalTime) * 1000));
                                    indexingRunnable.setInputExhausted();
                                    for (Thread thread : indexingThreads) {
                                        try {
                                            thread.join();
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    indexingThreads.clear();

//                                    queryRunnable.terminate();
//                                    for (Thread thread : queryThreads) {
//                                        thread.join();
//                                    }
//                                    queryThreads.clear();
//                                    queryRunnable = new QueryRunnable();

                                    System.out.println("total rebuild time " + totalRebuildTime);
                                    System.out.println("average rebuild time " + (totalRebuildTime * 1.0 / numberOfRebuild));
                                    System.out.println("number of rebuild " + numberOfRebuild);
//                        System.out.println("total time " + totalTime);
                                    System.out.println("average time in a chunk" + totalTime * 1.0 / numberOfChunks);
                                    System.out.println("average number of rebuild in a chunk " + numberOfRebuild * 1.0 / numberOfChunks);

                                    System.out.println(numberOfKeys * 1.0 / numberOfChunks);

                                    System.out.println(total);
//                                    System.out.println(totalQueryTime);
//                                    System.out.println("average query time " + totalQueryTime * 1.0 / numberOfQueries.get());
                                    break;
                                }
                            }
                        }


                    } catch(InterruptedException e){
                        e.printStackTrace();
                    } catch(IOException e){
                        e.printStackTrace();
                    }
                }
//                }
            }

        });
        emitThread.start();



//        populateInputQueueWithMoreTuples(5000);
        waitForInputQueueFilled();

//        System.out.println("Indexing threads have been created!!!");

        createIndexingThread();

        if (rebuildable) {
            checkingSkewnessRunnbale = new CheckingSkewnessRunnable();
            checkingSkewnessThread = new Thread(checkingSkewnessRunnbale);
            checkingSkewnessThread.start();
        }

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

    class CheckingSkewnessRunnable implements Runnable {

        boolean terminating = false;

        public void terminate() {
            terminating = true;
        }

        @Override
        public void run() {

            while (true) {

                if (terminating) {
                    break;
                }

//                    System.out.println(chunkId);
//                    System.out.println(executedInChekingThread.get());
//                    System.out.println(indexedData.getSkewnessFactor());
                if (chunkId.get() > 0 && estimatedDataSize.get() >= config.CHUNK_SIZE * config.SKEWNESS_DETECTION_THRESHOLD) {
//                        System.out.println("Before rebuilt " + indexedData.getSkewnessFactor());
//                    if (indexedData.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_THRESHOLD) {
//                    System.out.println(estimatedDataSize.get());
                        indexingRunnable.setInputExhausted();
                        for (Thread thread : indexingThreads) {
                            try {
                                thread.join();
//                                    System.out.println(String.format("Thread %d is joined!", thread.getId()));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        indexingThreads.clear();
                        indexingRunnable = new IndexingRunnable();



//                            System.out.println("begin to rebuild the template!!!");

                        long start = System.currentTimeMillis();

                        indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);

                        totalRebuildTime += (System.currentTimeMillis() - start);

//                            System.out.println("After rebuilt, the skewness is " + indexedData.getSkewnessFactor());

                        ++numberOfRebuild;

                        estimatedDataSize.set(0);

//                        System.out.println("New tree has been built");
//
//                            executedInChekingThread.set(0L);

//                            TopologyConfig.SKEWNESS_DETECTION_THRESHOLD = 0.01;
//
                        createIndexingThread();
//                    }
                }
            }
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
//                try {
                if(terminating) {
                    break;
                }


//                    Thread.sleep(1);
                Integer key = random.nextInt(2118413320);
//                    Thread.sleep(1);
                Integer leftKey = key;
                Integer rightKey = key;

                long start = System.currentTimeMillis();
                indexedData.searchRange(leftKey, rightKey);

//                    bulkLoader.pointSearch(indexValue);



                long time = System.currentTimeMillis() - start;
                numberOfQueries.incrementAndGet();
//                        indexedData.searchRange(leftKey, rightKey);
//                        s2.release();
//                        indexedData.searchRange(leftKey, rightKey);
                totalQueryTime += time;

//                    System.out.println(numberOfQueries.get());

//                    if (numberOfQueries.get() % 10000 == 0) {
//                        System.out.println("**********");
//                    }

//                if (numberOfQueries.get() == 5000) {
//                        double aveQueryTime = (double) totalTime.get() / (double) numberOfQueries;
//                        System.out.println(aveQueryTime);
//                        String content = "" + aveQueryTime;
//                        String newline = System.getProperty("line.separator");
//                        byte[] contentInBytes = content.getBytes();
//                        byte[] nextLineInBytes = newline.getBytes();
//                        System.out.println(String.format("%d queries executed!", numberOfQueries));
//                    System.out.println("latency per query: " + totalQueryTime / (double)5000 + " ms");
//                    totalQueryTime = 0L;
//                    numberOfQueries.set(0);
//                    break;
//                        totalTime = new AtomicLong(0);
//                }
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
            } else if (valueTypes.get(i).equals(Long.class)) {
                output.writeLong((Long) values.get(i));
            } else if (valueTypes.get(i).equals(Integer.class)) {
                output.writeLong((Integer) values.get(i));
            } else{
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
//        output.writeLong((Long) values.get(valueTypes.size()));
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
            indexedData = new BTree(btreeOrder, config);
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
                        final Integer indexValue = (Integer) pair.getKey();
//                        final Double indexValue = (Double) pair.getKey();
                        final byte[] serializedTuple = (byte[]) pair.getValue();
                        indexedData.insert(indexValue, serializedTuple);

                        estimatedDataSize.addAndGet(tupleLength + 21);
                    }
//                    executed.addAndGet(drainer.size());
//                    executedInChekingThread.addAndGet(drainer.size());
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

        NetworkTestIndexing test = new NetworkTestIndexing(bTreeOrder, 4, 4, true, false);
    }
}
