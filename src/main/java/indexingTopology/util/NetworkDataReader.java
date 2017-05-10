package indexingTopology.util;

import com.google.common.net.InetAddresses;
import indexingTopology.config.TopologyConfig;

import java.io.*;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by acelzj on 21/3/17.
 */
public class NetworkDataReader {
    public static void main(String[] args) throws IOException {
        int fileNumber = 1;

        BufferedReader bufferedReader = null;

        File file = new File(TopologyConfig.dataFileDir);


        bufferedReader = new BufferedReader(new FileReader(file));
//        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("/home/acelzj/Downloads/DPI/20150430_processed.txt")));

        String text = null;

        int maxFirstIP = Integer.MIN_VALUE;
        int minFirstIP = Integer.MAX_VALUE;

        int maxSecondIP = Integer.MIN_VALUE;
        int minSecondIP = Integer.MAX_VALUE;

        int maxThirdIP = Integer.MIN_VALUE;
        int minThirdIP = Integer.MAX_VALUE;

        System.out.println(maxThirdIP);
        System.out.println(minThirdIP);

//        System.out.println(maxFirstIP % 1000);

        int numberOfRecords = 0;

        long totalLength = 0;

        Map<Integer, Integer> IpToFrequencyMapping = new HashMap<>();

        while (true) {
            text = bufferedReader.readLine();
            ++numberOfRecords;
            if (text == null) {
                break;
            } else {
//                String dataPattern = "[[0-9]*-[0-9]*-[0-9]*\\s+([0-9]\\:)*[0-9]*\\.[0-9]*]";
//                System.out.println(text);

//                String line = "This order was placed for QT3000! OK?";
//                String pattern = "(.*)(\\d+)(.*)";


                String dataPattern = "\\[(\\d+-\\d+-\\d+\\s+\\d+:\\d+:\\d+\\.\\d+)\\]\\s+\\[(-?\\d+)\\]\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(http.*)";



                Pattern r = Pattern.compile(dataPattern);
//                Pattern r = Pattern.compile(pattern);

                Matcher m = r.matcher(text);
//                Matcher m = r.matcher(line);

                while (m.find()) {
//                    System.out.println("Found value: " + m.group(0));
//                    System.out.println("Found value: " + m.group(1));
//                    System.out.println("Found value: " + m.group(2));
//                    System.out.println("Found value: " + m.group(3));
                    InetAddress inetAddress = InetAddresses.forString(m.group(3));
//                    InetAddress inetAddress = InetAddresses.forString("0.1.0.0");
                    Integer sourceIP = InetAddresses.coerceToInteger(inetAddress);
                    if (sourceIP > maxFirstIP) {
                        maxFirstIP = sourceIP;
                    }

                    if (sourceIP < minFirstIP) {
                        minFirstIP = sourceIP;
                    }
//                    System.out.println("Found value: " + m.group(4));
//                    inetAddress = InetAddresses.forString(m.group(4));
//                    InetAddress inetAddress = InetAddresses.forString("0.1.0.0");
//                    sourceIP = InetAddresses.coerceToInteger(inetAddress);

//                    if (sourceIP > maxSecondIP) {
//                        maxSecondIP = sourceIP;
//                    }
//
//                    if (sourceIP < minSecondIP) {
//                        minSecondIP = sourceIP;
//                    }

                    inetAddress = InetAddresses.forString(m.group(5));
//                    inetAddress = InetAddresses.forString("255.0.0.0");
                    Integer destIP = InetAddresses.coerceToInteger(inetAddress);

                    int key = destIP / 10000;
                    if (IpToFrequencyMapping.get(key) != null) {
                        IpToFrequencyMapping.put(key, IpToFrequencyMapping.get(key) + 1);
                    } else {
                        IpToFrequencyMapping.put(key, 1);
                    }

//                    if (destIP < 0) {
//                        System.out.println(destIP);
//                    }

                    if (destIP > maxThirdIP) {
                        maxThirdIP = destIP;
                    }

                    if (destIP < minThirdIP) {
                        minThirdIP = destIP;
                    }

                    String s = m.group(6);

                    if (s.length() > TopologyConfig.AVERAGE_STRING_LENGTH) {
                        s = s.substring(0, TopologyConfig.AVERAGE_STRING_LENGTH);
                    }

//                    String textToWrite = "";
//                    textToWrite = textToWrite + sourceIP + " ";
//                    textToWrite = textToWrite + destIP + " ";
//                    textToWrite = textToWrite + s;
//
//                    bufferedWriter.write(textToWrite);
//                    bufferedWriter.newLine();
//                    bufferedWriter.flush();
//                    totalLength += m.group(6).length();

//                    System.out.println("Found value: " + m.group(6));
//                    System.out.println("Found value: " + m.group(7));
                }



            }
        }
//        System.out.println("First Ip");
//        System.out.println(minFirstIP);
//        System.out.println(maxFirstIP);

//        System.out.println("Second Ip");
//        System.out.println(minSecondIP);
//        System.out.println(maxSecondIP);

        System.out.println("Third Ip");
        System.out.println(minThirdIP);
        System.out.println(maxThirdIP);

        System.out.println(numberOfRecords);
        Object[] keys = IpToFrequencyMapping.keySet().toArray();
        Arrays.sort(keys);

        System.out.println("length " + keys.length);


//        System.out.println(keys[2]);
//        System.out.println(IpToFrequencyMapping.get(keys[1]));
//        System.out.println(IpToFrequencyMapping.get(keys[2]));
//        System.out.println(IpToFrequencyMapping.get(keys[3]));

//        for (double selectivity = 0.001; selectivity <= 0.1; selectivity *= 10) {
            int frequency = 0;
        double selectivity = 0.001;
            for (int i = 0; i < keys.length; ++i) {
                frequency += IpToFrequencyMapping.get(keys[i]);
                System.out.println(" " + ((Integer) keys[i]) * 10000 + " " + IpToFrequencyMapping.get(keys[i]));
                if (frequency >= numberOfRecords * selectivity) {
                    System.out.println("key " + (Integer) keys[0] * 10000);
                    System.out.println("frequency " + frequency);
                    System.out.println("key " + (Integer) keys[i] * 10000);
                    break;
                }
            }
//        }
    }
}
