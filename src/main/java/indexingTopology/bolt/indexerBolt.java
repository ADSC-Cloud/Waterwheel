package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
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
    private HdfsHandle hdfs;

    public IndexerBolt(String indexField,DataSchema schema, int btreeOrder, int bytesLimit) {
        this.schema=schema;
        this.indexField=indexField;
        this.btreeOrder=btreeOrder;
        this.bytesLimit = bytesLimit;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        indexedData=new BTree<Double>(btreeOrder);
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
            writeIndexedDataToHDFS();
//            indexedData=new BTree<Double>(btreeOrder);
            indexedData.clearPayload();
            try {
                indexedData.insert(indexValue,serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }

        try {
            collector.emit(schema.getValuesObject(tuple));
        } catch (IOException e) {
            e.printStackTrace();
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
        outputFieldsDeclarer.declare(schema.getFieldsObject());
    }
}
