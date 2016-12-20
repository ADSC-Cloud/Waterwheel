package indexingTopology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.commons.math3.distribution.NormalDistribution;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 7/27/16.
 */
public class TexiTrajectoryGenerator extends BaseRichSpout {

    SpoutOutputCollector collector_;
    File file;
    BufferedReader bufferedReader;
    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;


    public TexiTrajectoryGenerator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city)
            throws FileNotFoundException {
        this.schema = schema;
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_ = collector;
    }

    public void nextTuple() {
        try {
            String text = null;
            text = bufferedReader.readLine();
            if (text == null) {
//                bufferedReader.close();
                bufferedReader = new BufferedReader(new FileReader(file));
                text = bufferedReader.readLine();
            }
            String [] tuple = text.split(" ");
//            System.out.println("The tuple is " + schema.getValuesObject(tuple));
//            double indexValue = Double.parseDouble(text);
            Car car = generator.generate();
            collector_.emit(NormalDistributionIndexingTopology.IndexStream, new Values(car.id, city.getZCodeForALocation(car.x
            , car.y), new String(new char[payloadSize])), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
