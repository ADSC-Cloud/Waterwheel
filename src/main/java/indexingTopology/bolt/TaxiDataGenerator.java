package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by acelzj on 15/3/17.
 */
public class TaxiDataGenerator extends InputStreamReceiver {
    private City city;

    private BufferedReader bufferedReader = null;

    String fileName;

    List<Integer> generatorIds;

    int taskId;

    File folder;

    File[] listOfFiles;

    int step;

    int size;

    List<Integer> taxiIds;
    List<Double> longitudes;
    List<Double> latitudes;
    List<Integer> zcodes;

    private FrequencyRestrictor frequencyRestrictor;

    public TaxiDataGenerator(DataSchema schema, City city) {
        super(schema);
        this.city = city;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);

        generatorIds = topologyContext.getComponentTasks("TupleGenerator");

        size = generatorIds.size();

        taskId = topologyContext.getThisTaskId();

        folder = new File(TopologyConfig.dataFileDir);

        listOfFiles = folder.listFiles();

        step = 0;

        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        taxiIds = new ArrayList<>();
        zcodes = new ArrayList<>();

//        frequencyRestrictor = new FrequencyRestrictor(500000 / 24, 50);

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

                        final DataTuple dataTuple = new DataTuple(taxiId, zcode, longitude, latitude, timestamp);
                        try {
                            inputQueue.put(dataTuple);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
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
}
