package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final static int numThreads = 2;
    private int numSplit;
    private BulkLoader bulkLoader;
    private int dumplicateKeys;
    private int chunkId;
    private File file;
    private FileOutputStream fop;
    //  private LinkedList<Tuple> tuples;

    private class IndexerThread implements Runnable {
        private final BTree<Double,Integer> index;
        private final double indexValue;
        private final int offset;

        public IndexerThread(BTree<Double,Integer> index,double indexValue,int offset) {
            this.index = index;
            this.offset = offset;
            this.indexValue = indexValue;
        }

        public void run() {
            try {
                index.insert(indexValue,offset);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
    }

    public NormalDistributionIndexerBolt(DataSchema schema, int btreeOrder, int bytesLimit) {
        this.schema = schema;
    //    this.indexField = indexField;
        this.btreeOrder = btreeOrder;
        this.bytesLimit = bytesLimit;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        this.tm = TimingModule.createNew();
        this.sm = SplitCounterModule.createNew();
        indexedData = new BTree<Double,Integer>(btreeOrder,tm, sm);
        copyOfIndexedData = indexedData;
        chunk = MemChunk.createNew(this.bytesLimit);
        this.numTuples=0;
        this.numTuplesBeforeWritting = 0;
        this.numWritten=0;
        this.processingTime=0;
        this.bulkLoader = new BulkLoader(btreeOrder, tm, sm);
        this.chunkId = 0;




        //    file = new File("/home/acelzj/IndexTopology_experiment/insert_time_without_rebuild_but_split");
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/insert_time_with_rebuild_and_split_total");
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



        //  this.tuples = new LinkedList<Tuple>();
        es = Executors.newFixedThreadPool(1);

        try {
            hdfs = new HdfsHandle(map);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        es.shutdownNow();
    }

    public void execute(Tuple tuple) {
        try {
            //    tm.reset();
            //    tm.startTiming(Constants.TIME_SERIALIZATION_WRITE.str);
            Double indexValue = tuple.getDouble(0);
            byte [] serializedTuple = schema.serializeTuple(tuple);
            //   tuples.add(tuple);
            numTuples += 1;
            indexTupleWithTemplates(indexValue, serializedTuple);
            //   long total = tm.getTotal();
            //    processingTime += total;
            //    if (numSplit > 0) {
            //        System.out.println("numberOfSplit: " + numSplit);
            //   }
            //   System.out.println("num_tuples:" + numTuples + " , offset:" + offset + " , num_written:" + numWritten
            //            + " , " + tm.printTimes()+" , total:"+total+" , processingTotal:"+processingTime + " , numberOfSplit:" + numSplit);
//        collector.emit(new Values(numTuples,processingTime,templateTime,numFailedInsert,numWrittenTemplate));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void indexTupleWithTemplates(Double indexValue, byte[] serializedTuple) throws IOException{
        offset = chunk.write(serializedTuple);
        if (offset>=0) {
            //    tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
            if (bulkLoader.containsKey(indexValue)) {
                ++dumplicateKeys;
            }
            bulkLoader.addRecord(indexValue, offset);
            es.submit(new IndexerThread(indexedData, indexValue, offset));
        } else {
            shutdownAndRestartThreadPool(numThreads);



            writeIndexedDataToHDFS();
            numWritten++;
            int processedTuple = numTuples - numTuplesBeforeWritting;
            numTuplesBeforeWritting = numTuples;
            System.out.println("The chunk is full, split time is " + sm.getCounter() + ", " + processedTuple + "tuples has been processed");
            double percentage = (double) sm.getCounter() * 100 / (double) processedTuple;



            //   String content = "" + chunkId + " " + percentage;
         //   System.out.println("The total time is " + tm.getTotal());
         //   System.out.println(tm.getTotal());
         //   System.out.println(processedTuple);
         //   System.out.println(tm.getTotal() / processedTuple);
            double insertionTime = ((double) tm.getTotal()) / ((double) processedTuple);
            String content = "" + insertionTime;
         /*   double insertionTime = ((double) tm.getInsertionTime()) / ((double) processedTuple);
            double findTime = ((double) tm.getFindTime()) / ((double) processedTuple);
            double splitTime = ((double) tm.getSplitTime()) / ((double) processedTuple);*/
         //   String content = "" + tm.getTotal();
         //   String content = "insertion time is : " + insertionTime + " find time is : " + findTime + " split time is : " + splitTime;
            String newline = System.getProperty("line.separator");
            byte[] contentInBytes = content.getBytes();
            byte[] nextLineInBytes = newline.getBytes();

            fop.write(contentInBytes);
            fop.write(nextLineInBytes);
            System.out.println("The percentage of insert failure is " + percentage + "%");
            System.out.println(content);
            //    System.out.println
            System.out.println("Before rebuild the BTree: ");

            //   List keysBeforeRebuild = indexedData.printBtree();
            //   System.out.println("The number of record is " + bulkLoader.getNumberOfRecord());
            if (percentage > Config.rebuildTemplatePercentage) {
                indexedData = bulkLoader.createTreeWithBulkLoading();
                //   copyOfIndexedData = indexedData;
                System.out.println("New template has been built");
            }
        //    }// else {
            //    indexedData = copyOfIndexedData;
            // }
         /*   if (chunkId == 0) {
                try {
                    copyOfIndexedData = (BTree) indexedData.clone();
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    indexedData = (BTree) copyOfIndexedData.clone();
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
                indexedData.setSplitCounterModule(sm);
                indexedData.setTimingModule(tm);
            }*/
         /*   if (chunkId == 0) {
                System.out.println("The copy of BTree is: ");
                //    copyOfIndexedData = (BTree) BTree.deepClone(indexedData);
                //    copyOfIndexedData = (BTree) org.apache.commons.lang.SerializationUtils.clone(indexedData);
                try {
                    copyOfIndexedData = (BTree) indexedData.clone(indexedData);
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
                //    copyOfIndexedData.printBtree();
            } else {
                System.out.println("The copy of BTree is: ");
                //    copyOfIndexedData.printBtree();
                //    indexedData = (BTree) BTree.deepClone(copyOfIndexedData);
                //    copyOfIndexedData = (BTree) org.apache.commons.lang.SerializationUtils.clone(indexedData);
                try {
                    indexedData = (BTree) copyOfIndexedData.clone(copyOfIndexedData);
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
            }*/
            //   indexedData.printBtree();
            //   sm.resetCounter();
            //   indexedData.clearPayload();
            //   percentage = bulkLoader.checkNewTree(indexedData, sm);
            //   System.out.println("After rebuilt the tree, the insert failure is " + percentage);
            sm.resetCounter();
            tm.reset();
            bulkLoader.resetRecord();
            //    System.out.println("After rebuild the BTree: ");
            //   List keysAfterRebuild = indexedData.printBtree();
            //  for ()
            indexedData.clearPayload();
            //  System.out.println(sm.getCounter());
            offset = chunk.write(serializedTuple);
            //    tm.endTiming(Constants.TIME_SERIALIZATION_WRITE.str);
            //    dumplicateKeys = 0;
            bulkLoader.addRecord(indexValue, offset);
            ++chunkId;
            es.submit(new IndexerThread(indexedData,indexValue,offset));
        }
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

