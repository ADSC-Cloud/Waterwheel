package indexingTopology.util;

import indexingTopology.Config.TopologyConfig;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
import indexingTopology.FileSystemHandler.LocalFileSystemHandler;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by acelzj on 1/3/17.
 */
public class Indexer {

    private ArrayBlockingQueue<Pair> pendingQueue;

    private ArrayBlockingQueue<Pair> inputQueue;

    private BTree indexedData;

    private IndexingRunnable indexingRunnable;

    private List<Thread> indexingThreads;

    private TemplateUpdater templateUpdater;

    private Thread inputProcessingThread;

    private final static int numberOfIndexingThreads = 3;

    private AtomicLong executed;

    private int numTuples;

    private MemChunk chunk;

    private int chunkId;

    private long start;

    public Indexer(ArrayBlockingQueue<Pair> inputQueue, BTree indexedData, MemChunk chunk) {
        pendingQueue = new ArrayBlockingQueue<>(1024);

        this.inputQueue = inputQueue;

        this.indexedData = indexedData;

        templateUpdater = new TemplateUpdater(TopologyConfig.BTREE_OREDER, TimingModule.createNew(),
                SplitCounterModule.createNew());

        start = System.currentTimeMillis();

        inputProcessingThread = new Thread(new InputProcessingRunnable());

        inputProcessingThread.start();

        executed = new AtomicLong(0);

        numTuples = 0;

        chunkId = 0;

        indexingThreads = new ArrayList<>();

        this.chunk = chunk;

        createIndexingThread();
    }

    private void terminateIndexingThreads() {
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
            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }


    class InputProcessingRunnable implements Runnable {

        @Override
        public void run() {

            ArrayList<Pair> drainer = new ArrayList<Pair>();

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

                        terminateIndexingThreads();

                        indexedData = templateUpdater.createTreeWithBulkLoading(indexedData);

                        System.out.println("Time used to update template " + (System.currentTimeMillis() - start));
//
                        System.out.println("New tree has been built");
//
                        executed.set(0L);
//
                        createIndexingThread();
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

                    FileSystemHandler fileSystemHandler = null;
                    String fileName = null;

                    chunk.changeToLeaveNodesStartPosition();
                    indexedData.writeLeavesIntoChunk(chunk);

                    chunk.changeToStartPosition();
                    byte[] serializedTree = SerializationHelper.serializeTree(indexedData);
                    chunk.write(serializedTree);

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


                    System.out.println(String.format("Index throughput = %f tuple / s in a chunk", numTuples / (double) (System.currentTimeMillis() - start) * 1000));

                    indexedData.clearPayload();

                    createIndexingThread();

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

                for (Pair pair : drainer) {
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
}
