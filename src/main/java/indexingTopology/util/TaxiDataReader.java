package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.texi.City;
import javafx.util.Pair;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by acelzj on 15/3/17.
 */
public class TaxiDataReader {

    public static void main(String[] args) {

        int fileNumber = 1;

        BufferedReader bufferedReader = null;
        File folder = new File(TopologyConfig.dataFileDir);

        File[] listOfFiles = folder.listFiles();

        System.out.println(listOfFiles.length);
//        File file = new File(TopologyConfig.dataFileDir + "/" + fileNumber + ".txt");

        Double Xmin = Double.MAX_VALUE;
        Double Xmax = Double.MIN_VALUE;

        Double Ymin = Double.MAX_VALUE;
        Double Ymax = Double.MIN_VALUE;

        int minZCode = Integer.MAX_VALUE;
        int maxZCode = Integer.MIN_VALUE;

        Map<Double, Integer> longitudeHistogram = new HashMap<>();
        Map<Double, Integer> latitudeHistogram = new HashMap<>();

        Map<Integer, Integer> zcodeHistogram = new HashMap<>();

        final double x1 = 115.0;
        final double x2 = 117.0;
        final double y1 = 39.0;
        final double y2 = 41.0;
        final int partitions = 1024;

        City city = new City(x1, x2, y1, y2, partitions);

//        try {
//            bufferedReader = new BufferedReader(new FileReader(file));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

        Long numberOfRecord = 0L;

//        while (fileNumber <= 10357) {

        for (File file : listOfFiles) {
            try {
                bufferedReader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

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

//                    System.out.println(date.getTime());

                    Double x = Double.parseDouble(data[2]);
                    Double y = Double.parseDouble(data[3]);

                    if (x > Xmax) {
                        Xmax = x;
                    }

                    if (x < Xmin) {
                        Xmin = x;
                    }

                    if (y > Ymax) {
                        Ymax = y;
                    }

                    if (y < Ymin) {
                        Ymin = y;
                    }

                    ++numberOfRecord;


                    int zcode = city.getZCodeForALocation(x, y);

                    if (zcode < minZCode) {
                        minZCode = zcode;
                    }

                    if (zcode > maxZCode) {
                        maxZCode = zcode;
                    }

                    DecimalFormat df = new DecimalFormat("0.0");

                    x = Double.parseDouble(df.format(x));

                    if (longitudeHistogram.get(x) == null) {
                        longitudeHistogram.put(x, 1);
                    } else {
                        longitudeHistogram.put(x, longitudeHistogram.get(x) + 1);
                    }

                    y = Double.parseDouble(df.format(y));

                    if (latitudeHistogram.get(y) == null) {
                        latitudeHistogram.put(y, 1);
                    } else {
                        latitudeHistogram.put(y, latitudeHistogram.get(y) + 1);
                    }

                    if (zcodeHistogram.get(zcode / 10000) == null) {
                        zcodeHistogram.put(zcode / 10000, 1);
                    } else {
                        zcodeHistogram.put(zcode / 10000, zcodeHistogram.get(zcode / 10000) + 1);
                    }
                }
            }

        }

        System.out.println(numberOfRecord);

        System.out.println("Xmin " + Xmin);
        System.out.println("Xmax " + Xmax);

        System.out.println("Ymin " + Ymin);
        System.out.println("Ymax " + Ymax);

//        City city = new City(0.0, 300.0, 0.0, 100.0, 10000);

        System.out.println(city.getMaxZCode());

        System.out.println("max zcode " + maxZCode);
        System.out.println("min zcode " + minZCode);

        Object[] keys = longitudeHistogram.keySet().toArray();
        Arrays.sort(keys);

        for (int i = 0; i < keys.length; ++i) {
            System.out.println(keys[i] + " " + longitudeHistogram.get(keys[i]));
        }


        keys = latitudeHistogram.keySet().toArray();
        Arrays.sort(keys);

        for (int i = 0; i < keys.length; ++i) {
            System.out.println(keys[i] + " " + latitudeHistogram.get(keys[i]));
        }


        keys = zcodeHistogram.keySet().toArray();
        Arrays.sort(keys);

        for (int i = 0; i < keys.length; ++i) {
            System.out.println("" + (Integer) keys[i] * 10000 + " " + zcodeHistogram.get(keys[i]));
        }
    }
}
