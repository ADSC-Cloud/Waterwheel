package indexingTopology.bolt;

import indexingTopology.Config.TopologyConfig;
import indexingTopology.DataSchema;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import javafx.util.Pair;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by acelzj on 12/23/16.
 */
public class IndexServerTest {

    private final static int numberOfIndexingThreads = 3;

    private BufferedReader bufferedReader;

    private static DataSchema schema;

    private final int btreeOrder;
    private static int bytesLimit;

    private final String indexField;

    private static int chunkId;

    private static boolean isTreeBuilt = false;

    private static MemChunk chunk;

    private TimingModule tm;
    private static SplitCounterModule sm;

    private long processingTime;

    private long totalTime;

    private static TemplateUpdater templateUpdater;

    private Double minIndexValue = Double.MAX_VALUE;
    private Double maxIndexValue = Double.MIN_VALUE;

    private Long minTimeStamp = Long.MAX_VALUE;
    private Long maxTimeStamp = Long.MIN_VALUE;


    private static ArrayBlockingQueue<Pair> queue;

    private static BTree indexedData;

    private static List<Thread> indexingThreads = new ArrayList<Thread>();

    private static IndexingRunnable indexingRunnable;

    static List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
    static List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));

    public IndexServerTest(String indexField, DataSchema schema, int btreeOrder, int bytesLimit) {
        this.indexField = indexField;
        this.schema = schema;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
        this.queue = new ArrayBlockingQueue<Pair>(1024);
        this.tm = TimingModule.createNew();
        this.sm = SplitCounterModule.createNew();
        this.chunk = MemChunk.createNew(bytesLimit);
        this.templateUpdater = new TemplateUpdater(btreeOrder, tm, sm);
        indexedData = new BTree(btreeOrder, tm, sm);
    }


    private static void createGenerateThread() {
        Thread generateThread = new Thread(new GenerateRunnable());
        generateThread.start();
    }

    private static void createIndexingThread() {
        createIndexingThread(numberOfIndexingThreads);
    }

    private static void createIndexingThread(int n) {
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

    private static void terminateIndexingThreads() {
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

    public static void main(String[] args) {

        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

        final int btreeOrder = 4;

        final int bytesLimit = 65000000;

        IndexServerTest indexServerTest = new IndexServerTest("user_id", schema, btreeOrder, bytesLimit);

        createGenerateThread();

        createIndexingThread();

    }

    static class GenerateRunnable implements Runnable {
        @Override
        public void run() {
            final double x1 = 0;
            final double x2 = 1000;
            final double y1 = 0;
            final double y2 = 500;
            final int partitions = 100;

            final int payloadSize = 10;

            int numTuples = 0;

            TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
            City city = new City(x1, x2, y1, y2, partitions);
            while (true) {
                Car car = generator.generate();
                try {
                    int ZCode = city.getZCodeForALocation(car.x, car.y);
                    Long timestamp = System.currentTimeMillis();
                    Values values = new Values((double) car.id, (double) ZCode,
                            new String(new char[payloadSize]), timestamp);
                    byte[] serializedTuples = serializeValues(values);

                    ++numTuples;
                    if (numTuples < TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK) {
                        queue.put(new Pair((double) ZCode, serializedTuples));
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

                        chunk.changeToLeaveNodesStartPosition();
                        indexedData.writeLeavesIntoChunk(chunk);
                        chunk.changeToStartPosition();

                        byte[] serializedTree = SerializationHelper.serializeTree(indexedData);

                        chunk.write(serializedTree);

                        indexedData.clearPayload();

                        FileSystemHandler fileSystemHandler = null;
                        String fileName = null;
                        try {
                            if (TopologyConfig.HDFSFlag) {
                                fileSystemHandler = new HdfsFileSystemHandler(TopologyConfig.dataDir);
                            } else {
                                fileSystemHandler = new LocalFileSystemHandler(TopologyConfig.dataDir);
                            }
                            fileName = "chunk" + chunkId;
                            fileSystemHandler.writeToFileSystem(chunk, "/", fileName);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        numTuples = 0;
                        chunk = MemChunk.createNew(bytesLimit);
                        sm.resetCounter();
                        byte[] serializedTuple = serializeValues(values);
                        Pair pair = new Pair((double) ZCode, serializedTuple);
                        queue.put(pair);
                        createIndexingThread();
                        ++numTuples;
                        ++chunkId;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

        }

    }

    static class IndexingRunnable implements Runnable {

        boolean inputExhausted = false;

        public void setInputExhausted() {
            inputExhausted = true;
        }

        AtomicLong executed;
        AtomicLong totalExecuted;
        Long startTime;
        AtomicInteger threadIndex = new AtomicInteger(0);

        Lock lock;

        Object syn = new Object();

        @Override
        public void run() {
            boolean first = false;
            synchronized (syn) {
                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                    first = true;
                }
                if (executed == null)
                    executed = new AtomicLong(0);

                if (totalExecuted == null)
                    totalExecuted = new AtomicLong(0);

                if (lock == null)
                    lock = new ReentrantLock();
            }
            long localCount = 0;
            ArrayList<Pair> drainer = new ArrayList<Pair>();
            while (true) {
                try {
//                        Pair pair = queue.poll(1, TimeUnit.MILLISECONDS);
//                        if (pair == null) {
//                        if(!first)
//                            Thread.sleep(100);
                        try {
                            lock.lock();

                            int size = queue.size() > 256 ? 256 : queue.size();

                            if (executed.addAndGet(size) >= TopologyConfig.NUM_TUPLES_TO_CHECK_TEMPLATE) {
                                if (indexedData.getSkewnessFactor() >= TopologyConfig.REBUILD_TEMPLATE_PERCENTAGE) {
//                                    System.out.println(indexedData.getSkewnessFactor());
                                    Long start = System.currentTimeMillis();
                                    createNewTemplate();
//                                    System.out.println("rebuild time " + (System.currentTimeMillis() - start));
                                    executed.set(0L);
                                }
                            }

                            queue.drainTo(drainer, 256);
                            executed.addAndGet(drainer.size());

                        } finally {
                            lock.unlock();
                        }


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
//                            indexedData.insert(indexValue, offset);
                        }

                        totalExecuted.getAndAdd(drainer.size());

                        drainer.clear();

                } catch (UnsupportedGenericException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(first) {
                System.out.println(String.format("Index throughput = %f tuple / s", totalExecuted.get() / (double) (System.currentTimeMillis() - startTime) * 1000));
                System.out.println("Thread execution time: " + (System.currentTimeMillis() - startTime) + " ms.");
            }
//                System.out.println("Indexing thread " + Thread.currentThread().getId() + " is terminated with " + localCount + " tuples processed!");
        }
    }

    private static void createNewTemplate() {
        indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);
    }


    public static byte[] serializeValues(Values values) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i=0;i < valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            } else if (valueTypes.get(i).equals(String.class)) {
                byte [] b = ((String) values.get(i)).getBytes();
                byte [] sizeHeader = ByteBuffer.allocate(Integer.SIZE/ Byte.SIZE).putInt(b.length).array();
                bos.write(sizeHeader);
                bos.write(b);
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        //As we add timestamp for a field, so we need to serialize the timestamp
        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) values.get(valueTypes.size())).array();
        bos.write(b);

        return bos.toByteArray();
    }

    public static Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        int offset = 0;
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                int len = Double.SIZE/Byte.SIZE;
                double val = ByteBuffer.wrap(b, offset, len).getDouble();
                values.add(val);
                offset += len;
            } else if (valueTypes.get(i).equals(String.class)) {
                int len = Integer.SIZE/Byte.SIZE;
                int sizeHeader = ByteBuffer.wrap(b, offset, len).getInt();
                offset += len;
                len = sizeHeader;
                String val = new String(b, offset, len);
                values.add(val);
                offset += len;
            } else {
                throw new IOException("Only classes supported till now are string and double");
            }
        }

        int len = Long.SIZE / Byte.SIZE;
        Long val = ByteBuffer.wrap(b, offset, len).getLong();
        values.add(val);
        return values;
    }

}
