package indexingTopology.bolt;

import com.google.common.net.InetAddresses;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by acelzj on 30/3/17.
 */
public class NetworkDataGenerator extends InputStreamReceiver {

    BufferedReader bufferedReader = null;

    String dataPattern = "\\[(\\d+-\\d+-\\d+\\s+\\d+:\\d+:\\d+\\.\\d+)\\]\\s+\\[(-?\\d+)\\]\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(http.*)";

    Pattern r = Pattern.compile(dataPattern);

    FrequencyRestrictor frequencyRestrictor;

    private Thread generationThread;

    private TopologyConfig config;

    public NetworkDataGenerator(DataSchema schema, TopologyConfig config) {
        super(schema, config);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);

//        frequencyRestrictor = new FrequencyRestrictor(50000 / 24, 50);

        try {
            bufferedReader = new BufferedReader(new FileReader(new File(config.dataFileDir)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
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

                        Integer sourceIp = Integer.parseInt(data[0]);
                        Integer destIp = Integer.parseInt(data[1]);
                        String url = data[2];
                        Long timestamp = System.currentTimeMillis();

//                        try {
//                            frequencyRestrictor.getPermission(1);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

                        final DataTuple dataTuple = new DataTuple(sourceIp, destIp, url, timestamp);
                        try {
                            inputQueue.put(dataTuple);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
//                            e.printStackTrace();
                        }
                    }

                }
            }
        });
//        generationThread.start();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        generationThread.interrupt();
    }
}
