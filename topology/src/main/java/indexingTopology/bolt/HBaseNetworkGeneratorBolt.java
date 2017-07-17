package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.experiments.HBaseHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.metric.internal.RateTracker;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 4/4/17.
 */
public class HBaseNetworkGeneratorBolt extends BaseRichBolt {

    OutputCollector collector;

    private RateTracker rateTracker;

    BufferedReader bufferedReader = null;

    transient BufferedMutator table;

    String tableName = "Network";
    String columnFamilyName = "network";

    private transient HBaseHandler hBaseHandler;

    private List<Put> puts;

    int batchSize = 1;

    FrequencyRestrictor frequencyRestrictor;

    Long offset = (long) Integer.MAX_VALUE + 1;

    TopologyConfig config;

    public HBaseNetworkGeneratorBolt(TopologyConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        try {
            bufferedReader = new BufferedReader(new FileReader(new File(config.dataFileDir)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

//        frequencyRestrictor = new FrequencyRestrictor(50000 / 24, 50);

        rateTracker = new RateTracker(5 * 1000, 5);

        try {
            hBaseHandler = new HBaseHandler(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table = null;
        Connection connection = hBaseHandler.getConnection();
        try {
//            table = connection.getTable(TableName.valueOf(tableName));
            table = connection.getBufferedMutator(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        puts = new ArrayList<>();

        Thread generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    String text = null;
                    try {
                        text = bufferedReader.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (text == null) {
                        try {
                            bufferedReader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        try {
                            bufferedReader = new BufferedReader(new FileReader(new File(config.dataFileDir)));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    } else {
                        String[] data = text.split(" ");

                        Long sourceIp = (long) Integer.parseInt(data[0]) + offset;
                        Long destIp = (long) Integer.parseInt(data[1]) + offset;
                        String url = data[2];
                        Long timestamp = System.currentTimeMillis();

                        String rowKey = "" + String.format("%010d", destIp) + "-" + timestamp + "-" + String.format("%010d", sourceIp);

                        byte[] bytes = Bytes.toBytes(rowKey);

                        Put put = new Put(bytes);

                        try {
                            hBaseHandler.addLongValue(columnFamilyName, "sourceIp", bytes, sourceIp, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            hBaseHandler.addLongValue(columnFamilyName, "destIp", bytes, destIp, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        try {
                            hBaseHandler.addStringValue(columnFamilyName, "url", bytes, url, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                        try {
                            hBaseHandler.addLongValue(columnFamilyName, "timestamp", bytes, timestamp, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                        puts.add(put);

                        if (puts.size() == batchSize) {
                            try {
//                            table.put(batchPut);
//                                frequencyRestrictor.getPermission(1);
                                table.mutate(puts);
                                rateTracker.notify(batchSize);
                            } catch (IOException e) {
                                e.printStackTrace();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
                            }
                            puts.clear();
                        }


                    }

                }
            }
        });
        generationThread.start();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Streams.ThroughputRequestStream)) {
            collector.emit(Streams.ThroughputReportStream, new Values(rateTracker.reportRate()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Streams.ThroughputReportStream, new Fields("throughput"));
    }
}
