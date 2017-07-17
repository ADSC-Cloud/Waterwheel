package indexingTopology.util.experiments;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.TrajectoryGenerator;
import indexingTopology.util.taxi.TrajectoryUniformGenerator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.metric.internal.RateTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 27/2/17.
 */
public class HBaseTester {

    int numberOfIndexingThreads;

    private IndexingRunnable indexingRunnable;

    private List<Thread> indexingThreads;

    private AtomicInteger totalRecord;

    private HBaseHandler hBaseHandler;

    private int sleepTimeInSecond = 60;

    final int batchSize = 5000;

    RateTracker rateTracker = new RateTracker(50 * 1000, 50);

    String tableName = "TexiTable";
    String columnFamilyName = "Trajectory";


//    private int batchSize;

    public HBaseTester(int numberOfIndexingThreads, TopologyConfig config) throws Exception {
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        totalRecord = new AtomicInteger(0);
        indexingThreads = new ArrayList<>();
        hBaseHandler = null;
        try {
            hBaseHandler = new HBaseHandler(config);
        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            hBaseHandler.createTable(tableName, columnFamilyName, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long start = System.currentTimeMillis();
        createIndexingThread(24);

        Thread queryThread = new Thread(new QueryRunnable());
        queryThread.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(sleepTimeInSecond * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (true) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Throughput " + rateTracker.reportRate());
                    System.out.println("***************");
                }
            }
        }).start();

    }

    private void createIndexingThread(int n) {
        if(indexingRunnable == null) {
            indexingRunnable = new IndexingRunnable();
        }
        for(int i = 0; i < n; i++) {
            Thread indexThread = new Thread(indexingRunnable);
            indexThread.start();
//            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }

    class IndexingRunnable implements Runnable {

        @Override
        public void run() {


            final double x1 = 0;
            final double x2 = 1000;
            final double y1 = 0;
            final double y2 = 500;
            final int partitions = 100;
            final int payloadSize = 10;



//            Long numberOfRecord = 0L;

            final Long toTalRecord = 1000000L;

            Long timestamp = 0L;

            Random random = new Random(1000);

            TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);

            long duration = 0;

            Connection connection = hBaseHandler.getConnection();
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            List<Put> batchPut = new ArrayList<>();



            int numberOfPut = 0;

            Long start = System.currentTimeMillis();
            while (totalRecord.get() < 1500000000) {
                Car car = generator.generate();
                Double zcode = random.nextDouble();
                String s = new String(new char[payloadSize]);
//
                String rowKey = "" + String.format("%.8f", zcode) + "-" + String.format("%08d", System.currentTimeMillis()) + "-" + String.format("%5d", car.id);
//
//            String rowKey = "" + zcode;

                byte[] bytes = Bytes.toBytes(rowKey);



                Put put = new Put(bytes);



//            long start = System.currentTimeMillis();
                try {
                    hBaseHandler.addLongValue(columnFamilyName, "id", bytes, car.id, put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            System.out.println(System.currentTimeMillis() - start);
                try {
                    hBaseHandler.addDoubleValue(columnFamilyName, "zcode", bytes, zcode, put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//            System.out.println(System.currentTimeMillis() - start);
                try {
                    hBaseHandler.addStringValue(columnFamilyName, "payload", bytes, s, put);
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
                        table.put(batchPut);
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

                rateTracker.notify(1);
//
                ++timestamp;
//            hBaseHandler.search(tableName, columnFamilyName, columns, Bytes.toBytes(rowKey), Bytes.toBytes(rowKey));
            }

            if (batchPut.size() != 0) {
                try {
                    table.put(batchPut);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


//            duration = System.currentTimeMillis() - start;
//            System.out.println("Throughput : " + (toTalRecord * 1000 / duration) + " / s");


//        String startRowKey = "" + String.format("%.8f", 0.0) + "-" + String.format("%08d", 0);

//        String endRowKey = "" + String.format("%.8f", 0.1) + "-" + String.format("%08d", Long.MAX_VALUE);

//        List<String> columnNames = new ArrayList<>();
//        columnNames.add("id");
//        columnNames.add("zcode");
//        columnNames.add("payload");
//        columnNames.add("timestamp");

//        long start = System.currentTimeMillis();
//        hBaseHandler.search(table, columnFamilyName, columnNames, Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));
//        System.out.println(System.currentTimeMillis() - start);
            System.out.println("Throughput : " + (totalRecord.get() * 1000.0 / (System.currentTimeMillis() - start)));
        }



    }

    class QueryRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(sleepTimeInSecond * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Connection connection = hBaseHandler.getConnection();

            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (true) {
                Long endTimestamp = System.currentTimeMillis();
                String startRowKey = "" + String.format("%.8f", 0.0);
                String endRowKey = "" + String.format("%.8f", 0.01);


                Long startTimestamp = endTimestamp - sleepTimeInSecond * 1000;
//                System.out.println(startRowKey);
//                System.out.println(endRowKey);

                List<String> columnNames = new ArrayList<>();
                columnNames.add("id");
                columnNames.add("zcode");
                columnNames.add("payload");
                columnNames.add("timestamp");

                long start = System.currentTimeMillis();
                try {
                    hBaseHandler.search(table, columnFamilyName, columnNames, Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey), startTimestamp, endTimestamp, "timestamp");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(System.currentTimeMillis() - start);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyConfig config = new TopologyConfig();
        HBaseTester hBaseTester = new HBaseTester(8, config);
    }
}
