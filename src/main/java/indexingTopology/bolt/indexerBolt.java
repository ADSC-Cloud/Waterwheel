package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import indexingTopology.DataSchema;
import org.omg.CORBA.BAD_CONTEXT;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.Map;

/**
 * Created by parijatmazumdar on 17/09/15.
 */
public class IndexerBolt extends BaseRichBolt {
    OutputCollector collector;
    final DataSchema schema;

    private IndexerBolt() {
        schema=null;
    }

    public IndexerBolt(DataSchema schema) {
        this.schema=schema;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
    }

    public void execute(Tuple tuple) {
        try {
            collector.emit(schema.getValuesObject(tuple));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(schema.getFieldsObject());
    }
}
