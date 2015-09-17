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
    final String CSV_FILENAME;
    final DataSchema schema;

    public CSVReaderSpout(String CSV_FILENAME, DataSchema schema)
    {
        this.CSV_FILENAME = CSV_FILENAME;
        this.schema=schema;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(schema.getFieldsObject());
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_=collector;
    }

    public void nextTuple() {
        try {
            BufferedReader br=new BufferedReader(new FileReader(CSV_FILENAME));
            // skip header line
            String line=br.readLine();
            while ((line=br.readLine())!=null) {
                Utils.sleep(100);
                String [] tokens = line.split(",");
                collector_.emit(schema.getValuesObject(tokens));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
