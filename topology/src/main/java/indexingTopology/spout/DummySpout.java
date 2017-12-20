package indexingTopology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class DummySpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    Random rand;
    private String[] words = {"Hortonworks", "MapR", "Cloudera", "Hadoop", "Kafka", "Spark"};

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String word = words[rand.nextInt(words.length)];
        spoutOutputCollector.emit(new Values(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("OriginalWord"));
    }
}
