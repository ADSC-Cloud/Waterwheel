package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.BTree;
import indexingTopology.util.HdfsHandle;

import java.io.IOException;
import java.util.Map;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class IndexerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final DataSchema schema;
    private final String indexField;
    private final int btreeOrder;
    private final int bytesLimit;
    private BTree<Double> indexedData;
    private BTree<Double> planBIndex;
    private HdfsHandle hdfs;
    private int numTuples;
    private int numWritten;
    private long processingTime;
    private int numFailedInsert;

    public IndexerBolt(String indexField,DataSchema schema, int btreeOrder, int bytesLimit) {
        this.schema=schema;
        this.indexField=indexField;
        this.btreeOrder=btreeOrder;
        this.bytesLimit = bytesLimit;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        indexedData=new BTree<Double>(btreeOrder);
        planBIndex=new BTree<Double>(btreeOrder);
        this.numTuples=0;
        this.numWritten=0;
        this.processingTime=0;
        this.numFailedInsert =0;
        try {
            hdfs=new HdfsHandle(map);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        Double indexValue = tuple.getDoubleByField(indexField);
        byte[] serializedTuple=null;
        try {
            serializedTuple=schema.serializeTuple(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long startTime=System.nanoTime();
        numTuples+=1;
//        indexTuple(indexValue, serializedTuple);
        indexTupleWithTemplates(indexValue, serializedTuple);

        processingTime+=(System.nanoTime()-startTime);
        collector.emit(new Values(processingTime,numTuples,numWritten, numFailedInsert));
    }

    private void indexTupleWithTemplates(Double indexValue, byte[] serializedTuple) {
        try {
            int bytesEstimate=indexedData.getBytesEstimateForInsert(indexValue,serializedTuple);
            if (bytesEstimate<bytesLimit) {
                if (!indexedData.insert(indexValue,serializedTuple)) {
                    numFailedInsert++;
                    planBIndex.insert(indexValue, serializedTuple);
                }
            } else {
//            writeIndexedDataToHDFS();
                numWritten++;
                indexedData.clearPayload();
                planBIndex=new BTree<Double>(btreeOrder);
                indexedData.insert(indexValue,serializedTuple);
            }
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
    }

    private void indexTuple(Double indexValue, byte[] serializedTuple) {
        int bytesEstimate = 0;
        try {
            bytesEstimate=indexedData.getBytesEstimateForInsert(indexValue,serializedTuple);
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        if (bytesEstimate<bytesLimit) {
            try {
                indexedData.insert(indexValue,serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        } else {
//            writeIndexedDataToHDFS();
            numWritten++;
            indexedData=new BTree<Double>(btreeOrder);
            try {
                indexedData.insert(indexValue,serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeIndexedDataToHDFS() {
        try {
            hdfs.writeToNewFile(indexedData.serializeTree(),"testname"+System.currentTimeMillis()+".dat");
            System.out.println("**********************************WRITTEN*******************************");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("processing_time","num_tuples","num_written","num_failed_inserts"));
    }
}
