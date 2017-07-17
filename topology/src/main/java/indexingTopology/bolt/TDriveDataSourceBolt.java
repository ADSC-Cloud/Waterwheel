package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.taxi.City;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TDriveDataSourceBolt extends InputStreamReceiverBolt {
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

    private Thread generationThread;

    private String inputFilePath;

    private int maxInputRate;

    public TDriveDataSourceBolt(DataSchema schema, City city, TopologyConfig config, String inputFilePath, int maxInputRate) {
        super(schema, config);
        this.city = city;
        this.inputFilePath = inputFilePath;
        this.maxInputRate = maxInputRate;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);

        generatorIds = topologyContext.getComponentTasks("TupleGenerator");

        size = generatorIds.size();

        taskId = topologyContext.getThisTaskId();

        folder = new File(inputFilePath);

        listOfFiles = folder.listFiles();

        step = 0;

        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        taxiIds = new ArrayList<>();
        zcodes = new ArrayList<>();

        frequencyRestrictor = new FrequencyRestrictor(maxInputRate, 50);

        int index = 0;
//        double xmin, xmax, ymin, ymax;
//        xmin = 1000.0;
//        xmax = -1.0;
//        ymin = 1000.0;
//        ymax = -1.0;
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
                    Double longitude = Double.parseDouble(data[2]);
                    Double latitude = Double.parseDouble(data[3]);
//                    static final double x1 = 39.6;
//                    static final double x2 = 40.6;
//                    static final double y1 = 116.2;
//                    static final double y2 = 117.0;
                    if (longitude < 116 || longitude > 117 || latitude < 39.6 || latitude > 40.6)
                        continue;
                    taxiIds.add(taxiId);
                    longitudes.add(longitude);
                    latitudes.add(latitude);

                    int zcode = city.getZCodeForALocation(longitude, latitude);
                    zcodes.add(zcode);

//                    if (zcode == 5461) {
//                        xmin = Math.min(longitude, xmin);
//                        xmax = Math.max(longitude, xmax);
//                        ymin = Math.min(latitude, ymin);
//                        ymax = Math.max(latitude, ymax);
//                        System.out.println(String.format("(%f, %f) --> (%f, %f)", xmin, xmax, ymin, ymax));
//                    }

//                    if (new Random().nextInt(100000) ==0) {
//                    }
                    if (zcode > 16384) {
                        System.out.println("strange zCode: " + zcode);
                        System.out.println(String.format("%f, %f", longitude, latitude));
                    }
                }
            }
        }

        generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
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
//                    while (true) {
                    while (!Thread.currentThread().isInterrupted()) {
                        Integer taxiId = taxiIds.get(index);
                        Integer zcode = zcodes.get(index);
                        Double longitude = longitudes.get(index);
                        Double latitude = latitudes.get(index);
                        Long timestamp = System.currentTimeMillis();

                        try {
                            frequencyRestrictor.getPermission(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        final DataTuple dataTuple = new DataTuple(taxiId, zcode, longitude, latitude, timestamp);
                        try {
                            inputQueue.put(dataTuple);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
//                            e.printStackTrace();
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
    public void cleanup() {
        super.cleanup();
        generationThread.interrupt();
    }
}
