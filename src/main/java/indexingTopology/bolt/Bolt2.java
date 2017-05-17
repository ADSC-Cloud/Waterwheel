package indexingTopology.bolt;

import indexingTopology.metadata.FileMetaData;
import indexingTopology.metadata.HDFSHandler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Bolt2 extends BaseRichBolt {

    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        // prepare and emit FileMetaData information to HDFS bolt
        FileMetaData fileMetaData = HDFSHandler.generateFileMetaData();
        outputCollector.emit(HDFSHandler.emitToStream(), tuple, HDFSHandler.setValues(fileMetaData));

        // prepare and emit other information to default stream
        String appendedWord = tuple.getString(0) + "!!!";
        outputCollector.emit(tuple, new Values(appendedWord));

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(HDFSHandler.emitToStream(), HDFSHandler.setFields());
        outputFieldsDeclarer.declare(new Fields("AppendedWord"));
    }
}