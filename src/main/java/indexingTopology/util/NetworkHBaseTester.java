package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import indexingTopology.util.texi.City;
import org.apache.hadoop.hbase.TableName;
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

    private int sleepTimeInSecond = 5;

    final int batchSize = 5000;

    RateTracker rateTracker = new RateTracker(50 * 1000, 50);

    String tableName = "Network";
    String columnFamilyName = "network";


    BufferedReader bufferedReader = null;


//    private int batchSize;

    public NetworkHBaseTester(int numberOfIndexingThreads) throws Exception {
        this.numberOfIndexingThreads = numberOfIndexingThreads;
        totalRecord = new AtomicInteger(0);
        indexingThreads = new ArrayList<>();
        hBaseHandler = null;
        try {
            hBaseHandler = new HBaseHandler();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            hBaseHandler.creatTable(tableName, columnFamilyName, null);
        } catch (Exception e) {
            e.printStackTrace();
        }


        bufferedReader = new BufferedReader(new FileReader(new File(TopologyConfig.dataFileDir)));


        Long start = System.currentTimeMillis();
        createIndexingThread(numberOfIndexingThreads);

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

        @Override
        public void run() {

            Connection connection = hBaseHandler.getConnection();
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
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
                            bufferedReader = new BufferedReader(new FileReader(new File(TopologyConfig.dataFileDir)));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    String[] data = text.split(" ");

                    Integer sourceIp = Integer.parseInt(data[0]);
                    Integer destIp = Integer.parseInt(data[1]);
                    String url = data[2];
                    Long timestamp = System.currentTimeMillis();

                    String rowKey = "" + String.format("%010d", destIp) + "-" + timestamp + "-" + String.format("%010d", sourceIp);

//                    System.out.println(rowKey);

                    byte[] bytes = Bytes.toBytes(rowKey);

                    Put put = new Put(bytes);

                    try {
                        hBaseHandler.addIntValue(table, columnFamilyName, "sourceIp", bytes, sourceIp, put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        hBaseHandler.addIntValue(table, columnFamilyName, "destIp", bytes, destIp, put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    try {
                        hBaseHandler.addStringValue(table, columnFamilyName, "url", bytes, url, put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                    try {
                        hBaseHandler.addLongValue(table, columnFamilyName, "timestamp", bytes, timestamp, put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


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

                    totalRecord.incrementAndGet();

                    rateTracker.notify(1);

                }
            }


            if (batchPut.size() != 0) {
                try {
                    table.put(batchPut);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

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

                try {
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Long endTimestamp = System.currentTimeMillis();

                System.out.println(endTimestamp);

                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String startRowKey = "" + String.format("%010d", 236120000);
                String endRowKey = "" + String.format("%010d", 236800000);

                System.out.println(startRowKey);
                System.out.println(endRowKey);


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
            }
        }
    }


    public static void main(String[] args) throws Exception {
        NetworkHBaseTester networkHBaseTester = new NetworkHBaseTester(1);
    }
}
