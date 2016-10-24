package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import javafx.util.Pair;

import javax.sound.midi.SysexMessage;
import java.io.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by acelzj on 7/27/16.
 */
public class NormalDistributionIndexerBolt extends BaseRichBolt {
    private static final int maxTuples=43842;
    private OutputCollector collector;
    private final DataSchema schema;
    private final int btreeOrder;
    private final int bytesLimit;
    private BTree<Double, Integer> indexedData;
    private BTree<Double, Integer> copyOfIndexedData;
    private HdfsHandle hdfs;
    private int numTuples;
    private int numTuplesBeforeWritting;
    private int offset;
    private int numWritten;
    private MemChunk chunk;
    private TimingModule tm;
    private SplitCounterModule sm;
    private long processingTime;
    private ExecutorService es;
    private final static int NUMTHREADS = 2;
    //    private final static int numThreads = 1;
    private int numSplit;
    private BulkLoader bulkLoader;
    private final String indexField;
    private int dumplicateKeys;
    private int chunkId;
    private File file;
    private File inputFile;
    private File outputFile;
    private FileOutputStream fop;
    private FileOutputStream queryFileOutPut;
    private LinkedBlockingQueue<Pair> queue;
    private Thread insertThread;
    private Thread queryThread;
    private BufferedReader bufferedReader;
    private long totalTime;
    private int numberOfQueries;
    private Random random;

    public NormalDistributionIndexerBolt(String indexField, DataSchema schema, int btreeOrder, int bytesLimit) {
        this.schema = schema;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
        this.indexField = indexField;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        this.tm = TimingModule.createNew();
        this.sm = SplitCounterModule.createNew();
        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
        copyOfIndexedData = indexedData;
        chunk = MemChunk.createNew(this.bytesLimit);
        this.numTuples = 0;
        this.numTuplesBeforeWritting = 1;
        this.numWritten = 0;
        this.processingTime = 0;
        this.bulkLoader = new BulkLoader(btreeOrder, tm, sm);
        this.chunkId = 0;
        this.queue = new LinkedBlockingQueue<Pair>(1024);
        this.outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/query_latency_with_nothing_256");
//        this.outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/query_latency_without_rebuild_but_split_256");
//        this.outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/query_latency_with_rebuild_and_split_4");
        this.random = new Random(1000);
        this.numberOfQueries = 0;
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            queryFileOutPut = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.insertThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Pair pair = queue.take();
                        Double indexValue = (Double) pair.getKey();
                        Integer offset = (Integer) pair.getValue();
                        indexedData.insert(indexValue, offset);
                    } catch (UnsupportedGenericException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        this.insertThread.start();

        this.queryThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        long start = System.nanoTime();
                        Double indexValue = random.nextDouble() * 700 + 300;
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
                            queryFileOutPut.write(contentInBytes);
                            queryFileOutPut.write(nextLineInBytes);
                            numberOfQueries = 0;
                            totalTime = 0;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        this.queryThread.start();

//        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/specific_time_with_rebuild_and_split_with_query_4_64M");
//        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/specific_time_without_rebuild_but_split_with_query_256_64M");
        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/specific_time_with_nothing_256_64M");
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop = new FileOutputStream(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            hdfs = new HdfsHandle(map);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

  /*  public void execute(Tuple tuple) {
        try {
            Double indexValue = tuple.getDouble(0);
            tm.startTiming(Constants.TIME_SERIALIZATION_WRITE.str);
            byte[] serializedTuple = schema.serializeTuple(tuple);
            offset = chunk.write(serializedTuple);
            if (offset >= 0) {
                tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
                Pair pair = new Pair(indexValue, offset);
                queue.add(pair);
            } else {
                writeIndexedDataToHDFS();
                offset = chunk.write(serializedTuple);
                tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
                Pair pair = new Pair(indexValue, offset);
                queue.add(pair);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public void execute(Tuple tuple) {
        try {
            Double indexValue = tuple.getDoubleByField(indexField);
            byte [] serializedTuple = schema.serializeTuple(tuple);
            ++numTuples;
            indexTupleWithTemplates(indexValue, serializedTuple);
            collector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void indexTupleWithTemplates(final Double indexValue, byte[] serializedTuple) throws IOException, InterruptedException{

        offset = chunk.write(serializedTuple);
        if (offset >= 0) {
            Pair pair = new Pair(indexValue, offset);
            queue.put(pair);
            bulkLoader.addRecord(pair);
        } else {
            writeIndexedDataToHDFS();
            while (!queue.isEmpty()) {
                Utils.sleep(1);
            }
            int processedTuple = numTuples - numTuplesBeforeWritting;
            double insertionTime = ((double) tm.getInsertionTime()) / ((double) processedTuple);
            double locationTime = ((double) tm.getFindTime()) / ((double) processedTuple);
            double splitTime = ((double) tm.getSplitTime()) / ((double) processedTuple);
            String content = "" + locationTime + " " + insertionTime + " " + splitTime + " ";
            String newline = System.getProperty("line.separator");
            byte[] contentInBytes = content.getBytes();
            byte[] nextLineInBytes = newline.getBytes();
            fop.write(contentInBytes);
            fop.write(nextLineInBytes);
            numWritten++;
            System.out.println(content);
            numTuplesBeforeWritting = numTuples;
            double percentage = (double) sm.getCounter() * 100 / (double) processedTuple;
//            createNewTemplate(percentage);
//            copyTemplate(chunkId);
            createEmptyTree();
            indexedData.clearPayload();
            sm.resetCounter();
            tm.reset();
            bulkLoader.resetRecord();
            ++chunkId;
            offset = chunk.write(serializedTuple);
            Pair pair = new Pair(indexValue, offset);
            bulkLoader.addRecord(pair);
            queue.put(pair);
        }
    }

    private void copyTemplate(int chunkId) throws CloneNotSupportedException {
        if (chunkId == 0) {
            copyOfIndexedData = (BTree) indexedData.clone(indexedData);
        } else {
            indexedData = (BTree) copyOfIndexedData.clone(copyOfIndexedData);
        }
    }

    private void createEmptyTree() {
        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
    }

    // todo find a way to not shutdown threadpool everytime
    private void shutdownAndRestartThreadPool(int threads) {
        es.shutdown();
        try {
            es.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        es = Executors.newFixedThreadPool(threads);
    }



    private void createNewTemplate(double percentage) {
        if (percentage > Config.REBUILD_TEMPLATE_PERCENTAGE) {
            indexedData = bulkLoader.createTreeWithBulkLoading(indexedData);
        }
    }

    private void debugPrint(int numFailedInsert, Double indexValue) {
        if (numFailedInsert%1000==0) {
            System.out.println("[FAILED_INSERT] : "+indexValue);
            indexedData.printBtree();
        }
    }

/*
    private long buildOneTree(Double indexValue, byte[] serializedTuple) {
        if (numTuples<43842) {
            try {
                indexedData.insert(indexValue, serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }

        else if (numTuples==43842) {
            System.out.println("number of tuples processed : " + numTuples);
            System.out.println("**********************Tree Written***************************");
            indexedDataWoTemplate.printBtree();
            System.out.println("**********************Tree Written***************************");
        }

        return 0;
    }
*/

    private void indexTuple(Double indexValue, byte[] serializedTuple) {
        try {
            offset = chunk.write(serializedTuple);
            if (offset>=0) {
                tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
                indexedData.insert(indexValue, offset);
            } else {
                writeIndexedDataToHDFS();
                numWritten++;
                indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
                offset = chunk.write(serializedTuple);
                tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
                indexedData.insert(indexValue,offset);
            }
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
    }

    private void writeIndexedDataToHDFS() {
        // todo write this to hdfs
        chunk.serializeAndRefresh();
//        try {
//            hdfs.writeToNewFile(indexedData.serializeTree(),"testname"+System.currentTimeMillis()+".dat");
//            System.out.println("**********************************WRITTEN*******************************");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num_tuples","wo_template_time","template_time","wo_template_written","template_written"));
    }
}


