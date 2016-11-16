package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.DeserializationHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/9/16.
 */
public class ResultMergeBolt extends BaseRichBolt {

    Map<Double, Integer> keyToNumberOfTuples;

    DataSchema schema;

    OutputCollector collector;

    private int numberOfFiles;

    private int numberOfBolts;

    public ResultMergeBolt(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        keyToNumberOfTuples = new HashMap<Double, Integer>();
        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        System.out.println("The stream is " + tuple.getSourceStreamId());
//        if (tuple.getSourceStreamId() == NormalDistributionIndexingTopology.QueryInformationStream) {
//
//        }
        Double key = tuple.getDouble(0);
//        List<byte[]> serializedTuples = (List) tuple.getValue(1);
        ArrayList<byte[]> serializedTuples = (ArrayList) tuple.getValue(2);
        for (int i = 0; i < serializedTuples.size(); ++i) {
            Values deserializedTuple = null;
//            System.out.println("Serialized Tuples " + serializedTuples);
            try {
            deserializedTuple = schema.deserialize(serializedTuples.get(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(deserializedTuple);
        }
        Integer numberOfTuples = keyToNumberOfTuples.get(key);
        if (numberOfTuples == null)
            numberOfTuples = 0;
        numberOfTuples++;
        keyToNumberOfTuples.put(key, numberOfTuples);
        collector.ack(tuple);
//        System.out.println("Key: " + key + "Number of tuples: " + numberOfTuples);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
