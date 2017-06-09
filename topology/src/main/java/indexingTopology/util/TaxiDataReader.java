package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.taxi.City;

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

        TopologyConfig config = new TopologyConfig();

        BufferedReader bufferedReader = null;
        File folder = new File(config.dataFileDir);

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

        Map<Integer, Integer> zcodes = new HashMap<>();

        final double x1 = 116.2;
        final double x2 = 117.0;
        final double y1 = 39.6;
        final double y2 = 40.6;
        final int partitions = 1024;

        City city = new City(x1, x2, y1, y2, partitions);

//        try {
//            bufferedReader = new BufferedReader(new FileReader(file));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

        int numberOfRecord = 0;

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

                    if (zcodeHistogram.get(zcode / 1000) == null) {
                        zcodeHistogram.put(zcode / 1000, 1);
                    } else {
                        zcodeHistogram.put(zcode / 1000, zcodeHistogram.get(zcode / 1000) + 1);
                    }

                    if (zcodes.get(zcode) == null) {
                        zcodes.put(zcode, 1);
                    } else {
                        zcodes.put(zcode, zcodes.get(zcode) + 1);
                    }
                }
            }

        }

        System.out.println(numberOfRecord);

//        System.out.println("Xmin " + Xmin);
//        System.out.println("Xmax " + Xmax);

//        System.out.println("Ymin " + Ymin);
//        System.out.println("Ymax " + Ymax);

//        City city = new City(0.0, 300.0, 0.0, 100.0, 10000);

        System.out.println(zcodes.size());

        System.out.println(city.getMaxZCode());

//        System.out.println("max zcode " + maxZCode);
//        System.out.println("min zcode " + minZCode);

        Object[] keys = longitudeHistogram.keySet().toArray();
        Arrays.sort(keys);

//        int startIndex = 0;
//        int endIndex = 0;


//        for (int i = 0; i < keys.length; ++i) {
//            if ((Double) keys[i] == 116.3) {
//                startIndex = i;
//            }
//            if ((Double) keys[i] == 117.0) {
//                endIndex = i;
//            }

//            System.out.println(keys[i] + " " + longitudeHistogram.get(keys[i]));
//        }

//        int numRecords = 0;
//        for (int i = startIndex; i <= endIndex; ++i) {
//            numRecords += longitudeHistogram.get(keys[i]);
//        }
//
//        System.out.println(numRecords * 1.0 / numberOfRecord);



//        keys = latitudeHistogram.keySet().toArray();
//        Arrays.sort(keys);

//        for (int i = 0; i < keys.length; ++i) {
//            if ((Double) keys[i] == 39.7) {
//                startIndex = i;
//            }
//            if ((Double) keys[i] == 40.5) {
//                endIndex = i;
//            }
//            System.out.println(keys[i] + " " + latitudeHistogram.get(keys[i]));
//        }

//        numRecords = 0;
//        for (int i = startIndex; i <= endIndex; ++i) {
//            numRecords += latitudeHistogram.get(keys[i]);
//        }
//        System.out.println(numRecords * 1.0 / numberOfRecord);


        keys = zcodeHistogram.keySet().toArray();
        Arrays.sort(keys);

        System.out.println(keys[3]);

//        System.out.println(keys.length);

//        int startKey = (Integer) keys[0];

//        for (double selectivity = 0.001; selectivity <= 0.1; selectivity *= 10) {
            int frequency = 0;
            double selectivity = 0.001;

            int startIndex = 3;

            for (int i = startIndex;i < keys.length; ++i) {
//            if (((int) keys[i]) * 1000 >= 4000 && ((int) keys[i]) * 1000 <= 130000) {
//                frequency += zcodeHistogram.get(keys[i]);
//            }
//                System.out.println("" + (Integer) keys[i] * 1000 + " " + zcodeHistogram.get(keys[i]));
            frequency += zcodeHistogram.get(keys[i]);
//            System.out.println(zcodesInIndexing.keySet().size());
                if (frequency >= numberOfRecord * selectivity) {
//                    System.out.println(city.getMaxZCode() * selectivity);
                    System.out.println("key " + (Integer) keys[startIndex] * 1000);
                    System.out.println("frequency " + frequency);
                    System.out.println("key " + (Integer) keys[i] * 1000);
                    break;
                }
            }
//        }


//        System.out.println(frequency * 1.0 / numberOfRecord);

        /*
        selectivity = Math.sqrt(0.01);
        frequency = 0;
        for (int i = 0; i < keys.length; ++i) {
//            System.out.println("" + (Integer) keys[i] * 1000 + " " + zcodeHistogram.get(keys[i]));
            frequency += zcodeHistogram.get(keys[i]);
            if (frequency >= zcodesInIndexing.keySet().size() * selectivity) {
                System.out.println(zcodesInIndexing.keySet().size() * selectivity);
                System.out.println(frequency);
                System.out.println((Integer) keys[i] * 1000);
                break;
            }
        }

        selectivity = Math.sqrt(0.1);
        frequency = 0;
        for (int i = 0; i < keys.length; ++i) {
//            System.out.println("" + (Integer) keys[i] * 1000 + " " + zcodeHistogram.get(keys[i]));
            frequency += zcodeHistogram.get(keys[i]);
            if (frequency >= zcodesInIndexing.keySet().size() * selectivity) {
                System.out.println(zcodesInIndexing.keySet().size() * selectivity);
                System.out.println(frequency);
                System.out.println((Integer) keys[i] * 1000);
                break;
            }
        }
        */
    }
}
