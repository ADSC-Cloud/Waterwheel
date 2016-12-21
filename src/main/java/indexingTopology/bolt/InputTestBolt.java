package indexingTopology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by acelzj on 7/22/16.
 */
public class InputTestBolt extends BaseRichBolt {

    private OutputCollector collector;

    private int numTuples;

    private double startTime;
    public InputTestBolt() {
        numTuples = 0;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        startTime = System.nanoTime();
    }

    public void cleanup() {
        super.cleanup();
    }

    public void execute(Tuple tuple) {
        ++numTuples;
        double endTime = System.nanoTime();
        double emitTime = endTime - startTime;
        System.out.println("The emit time is " + emitTime);
        startTime = System.nanoTime();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
