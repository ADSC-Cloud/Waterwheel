package indexingTopology.util;

/**
 * Created by dmir on 9/26/16.
 */
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 9/26/16.
 */
public class TestIndexing {
    private LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
    private BTree<Double, Integer> indexedData;

    private final File inputFile;
    private final File outputFile;
    private final File queryOutputFile;

    private ByteArrayOutputStream bos;

    private SplitCounterModule sm;
    private TimingModule tm;
    private FileOutputStream fop;
    private FileOutputStream queryFileOutput;
    //    CopyOnWriteArrayList<Long> timer = new CopyOnWriteArrayList<Long>();
    private final int bytesLimit;
    private AtomicLong total;
    private final int btreeOrder;
    private int numTuples;
    private int chunkId;
    private double indexValue;
    private int numTuplesBeforeWritting;
    private int numberOfQueries;
    private long totalTime;

    private BulkLoader bulkLoader;
    private BTree<Double, Integer> copyOfIndexedData;
    private BufferedReader bufferedReader;
    private MemChunk chunk;

    private Random random;

    private List<String> fieldNames;
    private List<Class> valueTypes;

    private List<Double> values;

    public TestIndexing() {
        queue = new LinkedBlockingQueue<Pair>();

        btreeOrder = 4;
        chunkId = 0;
        total = new AtomicLong(0);
        numTuples = 0;
        numTuplesBeforeWritting = 1;
        bytesLimit = 65000000;

        inputFile = new File("/home/dmir/IndexTopology_experiment/NormalDistribution/input_data");
        outputFile = new File("/home/dmir/IndexTopology_experiment/NormalDistribution/total_time_thread_4");
        queryOutputFile = new File("/home/dmir/IndexTopology_experiment/NormalDistribution/query_latency_with_rebuild_4");

        chunk = MemChunk.createNew(bytesLimit);
        tm = TimingModule.createNew();
        sm = SplitCounterModule.createNew();

        indexedData = new BTree<Double,Integer>(btreeOrder, tm, sm);
        bulkLoader = new BulkLoader(btreeOrder, tm, sm);

        fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));

        random = new Random(1000);

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

        Thread emitThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    String text = null;
                    try {
                        text = bufferedReader.readLine();
                        String [] tokens = text.split(" ");
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
//                        createEmptyTree();
//                        copyTree();
                        createNewTree(percentage);
                        indexedData.clearPayload();
                        numTuplesBeforeWritting = numTuples;
                        long totalTime = total.get();
                        bulkLoader.resetRecord();

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
                        Pair pair = new Pair(indexValue, offset);
                        bulkLoader.addRecord(pair);
                        sm.resetCounter();
                        try {
                            queue.put(pair);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("In the emit thread, the chunkId is " + chunkId);
                        ++chunkId;
//                        tm.reset();
                        total = new AtomicLong(0);
                    }
                }
            }
        });
        emitThread.start();

        Thread indexThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    if (!queue.isEmpty()) {
                        try {
                            Pair pair = queue.take();
                            Double indexValue = (Double) pair.getKey();
                            Integer offset = (Integer) pair.getValue();
                            long start = System.nanoTime();
                            indexedData.insert(indexValue, offset);
                            total.addAndGet(System.nanoTime() - start);
                        } catch (UnsupportedGenericException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        indexThread.start();

        Thread queryThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        long start = System.nanoTime();
                        Double indexValue = random.nextDouble() * 700 + 300;
                        Thread.sleep(1);
//                        System.out.println("The chunk id is " + chunkId);
                        indexedData.search(indexValue);
                        long time = System.nanoTime() - start;
                        totalTime += time;
                        ++numberOfQueries;
                        if (numberOfQueries == 10000) {
                            double aveQueryTime = (double) totalTime / (double) numberOfQueries;
                            String content = "" + aveQueryTime;
                            String newline = System.getProperty("line.separator");
                            byte[] contentInBytes = content.getBytes();
                            byte[] nextLineInBytes = newline.getBytes();
                            queryFileOutput.write(contentInBytes);
                            queryFileOutput.write(nextLineInBytes);
                            numberOfQueries = 0;
                            totalTime = 0;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        queryThread.start();
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
        }
    }

    private void createNewTree(double percentage) {
//        int numberOfLeaves = bulkLoader.getNumberOfLeaves();
        if (percentage > Config.REBUILD_TEMPLATE_PERCENTAGE) {
            indexedData = bulkLoader.createTreeWithBulkLoading(indexedData);
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
        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
    }

    public static void main(String[] args) {
//        indexingTopology.TestIndexing test = new indexingTopology.TestIndexing(4, 0, false);
    }
}
