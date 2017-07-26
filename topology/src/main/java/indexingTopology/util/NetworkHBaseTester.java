package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.experiments.HBaseHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.metric.internal.RateTracker;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 31/3/17.
 */
public class NetworkHBaseTester {

    int numberOfIndexingThreads;

    private IndexingRunnable indexingRunnable;

    private List<Thread> indexingThreads;

    private AtomicInteger totalRecord;

    private HBaseHandler hBaseHandler;

    private int sleepTimeInSecond = 60;

    final int batchSize = 5000;

    Long offset = (long) Integer.MAX_VALUE + 1;

    RateTracker rateTracker = new RateTracker(5 * 1000, 50);

    String tableName = "Network";
    String columnFamilyName = "network";

    TopologyConfig config;
//    private int batchSize;

    public NetworkHBaseTester(int numberOfIndexingThreads, TopologyConfig config) throws Exception {
        this.config = config;
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        totalRecord = new AtomicInteger(0);
        indexingThreads = new ArrayList<>();
        hBaseHandler = null;
        try {
            hBaseHandler = new HBaseHandler(config);
        } catch (IOException e) {
            e.printStackTrace();
        }



//        try {
//            hBaseHandler.createTable(tableName, columnFamilyName, null);
//            System.out.println("created!!!");
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

//                    System.out.println("Throughput " + rateTracker.reportRate());
                    System.out.println(rateTracker.reportRate());
//                    System.out.println("***************");
                }
            }
        }).start();

    }

    private void createIndexingThread(int n) throws FileNotFoundException {
//        if(indexingRunnable == null) {
//            indexingRunnable = new IndexingRunnable();
//        }
        for(int i = 0; i < n; i++) {
            Thread indexThread = new Thread(new IndexingRunnable());
            indexThread.start();
//            System.out.println(String.format("Thread %d is created!", indexThread.getId()));
            indexingThreads.add(indexThread);
        }
    }

    class IndexingRunnable implements Runnable {

        BufferedReader bufferedReader;

        IndexingRunnable() throws FileNotFoundException {
        }

        @Override
        public void run() {

            Connection connection = hBaseHandler.getConnection();
            try {
                this.bufferedReader = new BufferedReader(new FileReader(new File(config.metadataDir)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
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

            while (totalRecord.get() < 1500000000) {
                String text = null;
                try {
                    text = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (text == null) {
                    try {
                        bufferedReader.close();
                        try {
                            bufferedReader = new BufferedReader(new FileReader(new File(config.metadataDir)));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    String[] data = text.split(" ");

                    Long sourceIp = (long) Integer.parseInt(data[0]) + offset;
                    Long destIp = (long) Integer.parseInt(data[1]) + offset;
                    String url = data[2];
                    Long timestamp = System.currentTimeMillis();



                    String rowKey = "" + String.format("%010d", destIp) + "-" + timestamp + "-" + String.format("%010d", sourceIp);

//                    System.out.println(rowKey);

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


                    batchPut.add(put);
                    numberOfPut++;

                    if (numberOfPut == batchSize) {
                        try {
//                            table.put(batchPut);
                            table.mutate(batchPut);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        numberOfPut = 0;
                        batchPut.clear();
                    }

                    totalRecord.incrementAndGet();

                    rateTracker.notify(1);

                }
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
//            try {
//                Thread.sleep(sleepTimeInSecond * 1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

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

                Long leftKey = 1074640000L + offset;
                Long rightKey = 1709410000L + offset;


                String startRowKey = "" + String.format("%010d", leftKey);
//                String startRowKey = "" + String.format("%011d", 1074640000);
                String endRowKey = "" + String.format("%010d", rightKey);
//                String endRowKey = "" + String.format("%011d", 1709410000);


                Long startTimestamp = endTimestamp - sleepTimeInSecond * 1000;
//                System.out.println(startRowKey);
//                System.out.println(endRowKey);

                List<String> columnNames = new ArrayList<>();
                columnNames.add("sourceIp");
                columnNames.add("destIp");
                columnNames.add("url");
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
        NetworkHBaseTester networkHBaseTester = new NetworkHBaseTester(24, new TopologyConfig());
    }
}
