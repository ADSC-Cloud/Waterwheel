package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.taxi.City;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.metric.internal.RateTracker;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 31/3/17.
 */
public class TaxiHBaseTester {

    int numberOfIndexingThreads;

    private IndexingRunnable indexingRunnable;

    private List<Thread> indexingThreads;

    private AtomicInteger totalRecord;

    private HBaseHandler hBaseHandler;

    private int sleepTimeInSecond = 5;

    final int batchSize = 1;

    RateTracker rateTracker = new RateTracker(5 * 1000, 50);

    String tableName = "TaxiTable";
    String columnFamilyName = "Beijing";

    List<Integer> taxiIds = new ArrayList<>();
    List<Double> longitudes = new ArrayList<>();
    List<Double> latitudes = new ArrayList<>();
    List<Integer> zcodes = new ArrayList<>();

    File folder = new File(TopologyConfig.dataFileDir);
    File[] listOfFiles = folder.listFiles();

    BufferedReader bufferedReader = null;


//    private int batchSize;

    public TaxiHBaseTester(int numberOfIndexingThreads) throws Exception {
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        totalRecord = new AtomicInteger(0);
        indexingThreads = new ArrayList<>();
        hBaseHandler = null;
        try {
            hBaseHandler = new HBaseHandler();
        } catch (IOException e) {
            e.printStackTrace();
        }


        final double x1 = 116.2;
        final double x2 = 117.0;
        final double y1 = 39.6;
        final double y2 = 40.6;
        final int partitions = 1024;

        City city = new City(x1, x2, y1, y2, partitions);



        for (int i = 0; i < listOfFiles.length; ++i) {
            File file = listOfFiles[i];
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





//        try {
//            hBaseHandler.createTable(tableName, columnFamilyName, null);
//            System.out.println("created!!");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }



        Long start = System.currentTimeMillis();
        createIndexingThread(numberOfIndexingThreads);

//        Thread queryThread = new Thread(new QueryRunnable());
//        queryThread.start();


        new Thread(new Runnable() {
            @Override
            public void run() {
//                try {
//                    Thread.sleep(sleepTimeInSecond * 1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                while (true) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//
                    System.out.println(rateTracker.reportRate());
                }
            }
        }).start();

    }

    private void createIndexingThread(int n) {
//        if(indexingRunnable == null) {
//            indexingRunnable = new IndexingRunnable();
//        }
        for(int i = 0; i < n; i++) {
            Thread indexThread = new Thread(new IndexingRunnable(i));
            indexThread.start();
//            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }

    class IndexingRunnable implements Runnable {

        int id;
        int step;

        List<Double> latitudesInIndexing = new ArrayList<>();
        List<Double> longitudesInIndexing = new ArrayList<>();
        List<Integer> taxiIdsInIndexing = new ArrayList<>();
        List<Integer> zcodesInIndexing = new ArrayList<>();

        final double x1 = 116.2;
        final double x2 = 117.0;
        final double y1 = 39.6;
        final double y2 = 40.6;
        final int partitions = 1024;

        City city = new City(x1, x2, y1, y2, partitions);

        public IndexingRunnable(int id) {
            this.id = id;
            step = 0;
        }

        @Override
        public void run() {

            int index = 0;
            while (index < taxiIds.size()) {
                index = id + step * numberOfIndexingThreads;
                if (index >= listOfFiles.length) {
                    break;
                }

                ++step;

//                while (index < taxiIdsInIndexing.size()) {
                        Integer taxiId = taxiIds.get(index);
                        taxiIdsInIndexing.add(taxiId);

                        Double longitude = longitudes.get(index);
                        longitudesInIndexing.add(longitude);

                        Double latitude = latitudes.get(index);
                        latitudesInIndexing.add(latitude);

                        int zcode = city.getZCodeForALocation(longitude, latitude);
                        zcodesInIndexing.add(zcode);
//                }
            }


            Connection connection = hBaseHandler.getConnection();
//            Table table = null;
            BufferedMutator table = null;
            try {
//                table = connection.getTable(TableName.valueOf(tableName));
                table = connection.getBufferedMutator(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }


            List<Put> batchPut = new ArrayList<>();

            int numberOfPut = 0;

            index = 0;
            while (totalRecord.get() < 1500000000) {
                Integer taxiId = taxiIdsInIndexing.get(index);
                Double latitude = latitudesInIndexing.get(index);
                Double longitude = longitudesInIndexing.get(index);
                Integer zcode = zcodesInIndexing.get(index);
                Long timestamp = System.currentTimeMillis();

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

//            duration += (System.currentTimeMillis() - start);

//            Long start = System.currentTimeMillis();
//            table.put(put);
                batchPut.add(put);
                numberOfPut++;

                if (numberOfPut == batchSize) {
                    try {
//                        table.put(batchPut);
                        table.mutate(batchPut);
                        rateTracker.notify(batchSize);
//                        table.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    numberOfPut = 0;
                    batchPut.clear();
                }

//            System.out.println("Put " + (System.currentTimeMillis() - start));

//            System.out.println(duration);

//            List<String> columns = new ArrayList<>();
//            columns.add("id");
//            columns.add("zcode");
//            columns.add("payload");
//
                totalRecord.addAndGet(1);


//
                ++index;
                if (index >= taxiIdsInIndexing.size()) {
                    index = 0;
                }
//            hBaseHandler.search(tableName, columnFamilyName, columns, Bytes.toBytes(rowKey), Bytes.toBytes(rowKey));
            }

            if (batchPut.size() != 0) {
                try {
//                    table.put(batchPut);
                    table.mutate(batchPut);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }



        }



    }

    class QueryRunnable implements Runnable {

        @Override
        public void run() {

//            Long startTime = System.currentTimeMillis();

//            Long endTimestamp = startTime;

            try {
                Thread.sleep(sleepTimeInSecond * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("begin search");

            Connection connection = hBaseHandler.getConnection();

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            Long queryId = 0L;


            while (queryId < 20) {

                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Long endTimestamp = System.currentTimeMillis();
//                Long endTimestamp = System.currentTimeMillis() - 300 * 1000;
                String startRowKey = "" + String.format("%07d", 3000);
//                String endRowKey = "" + String.format("%07d", 17000);
//                String endRowKey = "" + String.format("%07d", 52000);
                String endRowKey = "" + String.format("%07d", 108000);


                Long startTimestamp = endTimestamp - sleepTimeInSecond * 1000;
//                System.out.println(startRowKey);
//                System.out.println(endRowKey);

                List<String> columnNames = new ArrayList<>();
                columnNames.add("id");
                columnNames.add("zcode");
                columnNames.add("latitude");
                columnNames.add("longitude");
                columnNames.add("timestamp");

                long start = System.currentTimeMillis();
                try {
                    hBaseHandler.search(table, columnFamilyName, columnNames, Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey), startTimestamp, endTimestamp, "timestamp");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(System.currentTimeMillis() - start);

                ++queryId;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        TaxiHBaseTester taxiHBaseTester = new TaxiHBaseTester(24);
    }
}
