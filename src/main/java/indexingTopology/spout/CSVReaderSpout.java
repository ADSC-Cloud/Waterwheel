package indexingTopology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import indexingTopology.DataSchema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class CSVReaderSpout extends BaseRichSpout {
    SpoutOutputCollector collector_;
    private final DataSchema schema;
    private final String CSV_FILENAME;
    private BufferedReader bufRead;
    transient BufferedReader brtest;

    public CSVReaderSpout(String CSV_FILENAME, DataSchema schema)
    {
        this.schema=schema;
        this.CSV_FILENAME=CSV_FILENAME;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema.getFieldsObject());
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_=collector;
        Utils.sleep(60000);
        try {
            bufRead=new BufferedReader(new FileReader(CSV_FILENAME));
            // skip header
            bufRead.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            String line=bufRead.readLine();
            if (line!=null) {
                Utils.sleep(100);
                String [] tokens = line.split(",");
                collector_.emit(schema.getValuesObject(tokens));
            } else {
                Utils.sleep(5000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
