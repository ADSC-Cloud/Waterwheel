package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.HBaseHandler;
import indexingTopology.util.texi.City;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
public class HBaseTaxiGenerator extends BaseRichBolt{

    private City city;

    private BufferedReader bufferedReader = null;

    private RateTracker rateTracker;

    String fileName;

    List<Integer> generatorIds;

    int taskId;

    File folder;

    File[] listOfFiles;

    int step;

    int size;

    OutputCollector collector;

    List<Integer> taxiIds;
    List<Double> longitudes;
    List<Double> latitudes;
    List<Integer> zcodes;

//    transient Table table;
    transient BufferedMutator table;

    String tableName = "TaxiTable";
    String columnFamilyName = "Beijing";

    private int batchSize = 1;

    private List<Put> puts;

    private transient HBaseHandler hBaseHandler;

    FrequencyRestrictor frequencyRestrictor;

    public HBaseTaxiGenerator(City city) {
        this.city = city;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        rateTracker = new RateTracker(5 * 1000, 5);

        generatorIds = context.getComponentTasks("IndexerBolt");

        size = generatorIds.size();

        folder = new File(TopologyConfig.dataFileDir);

        listOfFiles = folder.listFiles();

        frequencyRestrictor = new FrequencyRestrictor(50000 / 24, 50);

        step = 0;

        hBaseHandler = null;
        try {
            hBaseHandler = new HBaseHandler();
        } catch (IOException e) {
            e.printStackTrace();
        }

        taskId = context.getThisTaskId();

//        if (generatorIds.indexOf(taskId) == 0) {
//            try {
//                hBaseHandler.createTable(tableName, columnFamilyName, null);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        table = null;
        Connection connection = hBaseHandler.getConnection();
        try {
//            table = connection.getTable(TableName.valueOf(tableName));
            table = connection.getBufferedMutator(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }



        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        taxiIds = new ArrayList<>();
        zcodes = new ArrayList<>();

        puts = new ArrayList<>();

        int index = 0;
        while (true) {
            index = generatorIds.indexOf(taskId) + step * size;
            if (index >= listOfFiles.length) {
                break;
            }

            File file = listOfFiles[index];
            try {
                bufferedReader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            ++step;

            while (true) {
                String text = null;
                try {
                    text = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (text == null) {
                    break;
                } else {
                    String[] data = text.split(",");

                    Integer taxiId = Integer.parseInt(data[0]);
                    taxiIds.add(taxiId);

                    Double longitude = Double.parseDouble(data[2]);
                    longitudes.add(longitude);

                    Double latitude = Double.parseDouble(data[3]);
                    latitudes.add(latitude);

                    int zcode = city.getZCodeForALocation(longitude, latitude);
                    zcodes.add(zcode);
                }
            }
        }

        Thread generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    /*
                    try {
                        int index = generatorIds.indexOf(taskId) + step * size;
                        index = index % listOfFiles.length;

                        File file = listOfFiles[index];
                        bufferedReader = new BufferedReader(new FileReader(file));

                        ++step;

//                        System.out.println(index);

                        while (true) {
                            String text = null;
                            try {
                                text = bufferedReader.readLine();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            if (text == null) {
                                break;
                            } else {
                                String[] data = text.split(",");

                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-DD hh:mm:ss");
                                Date date = null;
                                try {
                                    date = simpleDateFormat.parse(data[1]);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }

                                Integer taxiId = Integer.parseInt(data[0]);
                                Long timestamp = date.getTime();

                                Double x = Double.parseDouble(data[2]);
                                Double y = Double.parseDouble(data[3]);

                                int zcode = city.getZCodeForALocation(x, y);

//                                if (x >= 115 && x <= 117 && y >= 39 && y <= 40) {

                                    final DataTuple dataTuple = new DataTuple(taxiId, zcode, x, y, timestamp);

                                    inputQueue.put(dataTuple);
//                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    */
                    int index = 0;
                    while (true) {
                        Integer taxiId = taxiIds.get(index);
                        Integer zcode = zcodes.get(index);
                        Double longitude = longitudes.get(index);
                        Double latitude = latitudes.get(index);
                        Long timestamp = System.currentTimeMillis();

//                        try {
//                            frequencyRestrictor.getPermission(1);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

                        String rowKey = "" + String.format("%07d", zcode) + "-" + timestamp + "-" + String.format("%05d", taxiId);

                        byte[] bytes = Bytes.toBytes(rowKey);

                        Put put = new Put(bytes);

                        try {
                            hBaseHandler.addIntValue(columnFamilyName, "id", bytes, taxiId, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            hBaseHandler.addIntValue(columnFamilyName, "zcode", bytes, zcode, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        try {
                            hBaseHandler.addDoubleValue(columnFamilyName, "latitude", bytes, latitude, put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        try {
                            hBaseHandler.addDoubleValue(columnFamilyName, "longitude", bytes, longitude, put);
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
//                                table.put(put);
                                frequencyRestrictor.getPermission(1);
                                table.mutate(put);
                                rateTracker.notify(batchSize);
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            puts.clear();
                        }



                        ++index;
                        if (index >= taxiIds.size()) {
                            index = 0;
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
