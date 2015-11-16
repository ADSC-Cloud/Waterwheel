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
    private BTree<Double> indexedDataWoTemplate;
    private BTree<Double> indexedDataWithTemplate;
    private BTree<Double> planBIndex;
    private HdfsHandle hdfs;
    private int numTuples;
    private int numWrittenTemplate;
    private int numWrittenWoTemplate;
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
        indexedDataWoTemplate=new BTree<Double>(btreeOrder);
        indexedDataWithTemplate=new BTree<Double>(btreeOrder);
        planBIndex=new BTree<Double>(btreeOrder);
        this.numTuples=0;
        this.numWrittenTemplate=0;
        this.numWrittenWoTemplate=0;
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

        numTuples+=1;
//        long woTemplateTime=indexTuple(indexValue, serializedTuple);
        long templateTime=indexTupleWithTemplates(indexValue, serializedTuple);
        processingTime+=templateTime;
        collector.emit(new Values(numTuples,processingTime,templateTime,numFailedInsert,numWrittenTemplate));
    }

    private long indexTupleWithTemplates(Double indexValue, byte[] serializedTuple) {
        long startTime=System.nanoTime();
        try {
            int bytesEstimate=indexedDataWithTemplate.getBytesEstimateForInsert(indexValue,serializedTuple);
            if (bytesEstimate<bytesLimit) {
                if (!indexedDataWithTemplate.insert(indexValue,serializedTuple)) {
                    planBIndex.insert(indexValue, serializedTuple);
                } else
                    numFailedInsert++;
            } else {
//            writeIndexedDataToHDFS();
                numWrittenTemplate++;
                indexedDataWithTemplate.clearPayload();
                planBIndex=new BTree<Double>(btreeOrder);
                indexedDataWithTemplate.insert(indexValue,serializedTuple);
            }
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        return System.nanoTime()-startTime;
    }

    private long indexTuple(Double indexValue, byte[] serializedTuple) {
        long startTime=System.nanoTime();
        int bytesEstimate=0;
        try {
            bytesEstimate=indexedDataWoTemplate.getBytesEstimateForInsert(indexValue,serializedTuple);
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

        if (bytesEstimate<bytesLimit) {
            try {
                indexedDataWoTemplate.insert(indexValue,serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        } else {
//            writeIndexedDataToHDFS();
            numWrittenWoTemplate++;
            indexedDataWoTemplate=new BTree<Double>(btreeOrder);
            try {
                indexedDataWoTemplate.insert(indexValue,serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }

        return System.nanoTime()-startTime;
    }

    private void writeIndexedDataToHDFS() {
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
