package indexingTopology.util;

import com.esotericsoftware.kryo.io.Output;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by acelzj on 27/2/17.
 */
public class HBaseTester {

    public static void main(String[] args) throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();

        String tableName = "TexiTable";
        String columnFamilyName = "Trajectory";

//        hBaseHandler.creatTable(tableName, columnFamilyName, null);

        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;
        final int payloadSize = 10;

        Long numberOfRecord = 0L;

        final Long toTalRecord = 1L * 50000;

        Long timestamp = 0L;

        Random random = new Random(1000);

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);

        long duration = 0;

//        while (numberOfRecord < toTalRecord) {
//            Car car = generator.generate();
//            Double zcode = random.nextDouble();
//            String s = new String(new char[payloadSize]);
//
//            String rowKey = "" + String.format("%.8f", zcode) + "-" + String.format("%08d", timestamp) + "-" + String.format("%5d", car.id);
//
//            String rowKey = "" + zcode;

//            byte[] bytes = Bytes.toBytes(rowKey);

            Connection connection = hBaseHandler.getConnection();

            Table table = connection.getTable(TableName.valueOf(tableName));

//            Put put = new Put(bytes);

//            long start = System.currentTimeMillis();
//            hBaseHandler.addLongValue(table, columnFamilyName, "id", bytes, car.id, put);
//            System.out.println(System.currentTimeMillis() - start);
//            hBaseHandler.addDoubleValue(table, columnFamilyName, "zcode", bytes, zcode, put);
//            System.out.println(System.currentTimeMillis() - start);
//            hBaseHandler.addStringValue(table, columnFamilyName, "payload", bytes, s, put);
//            duration += (System.currentTimeMillis() - start);

//            table.put(put);

//            System.out.println(duration);

//            List<String> columns = new ArrayList<>();
//            columns.add("id");
//            columns.add("zcode");
//            columns.add("payload");
//
//            ++numberOfRecord;
//
//            ++timestamp;
//            hBaseHandler.search(tableName, columnFamilyName, columns, Bytes.toBytes(rowKey), Bytes.toBytes(rowKey));
//        }

//        long duration = System.currentTimeMillis() - start;
//        System.out.println("Throughput : " + (toTalRecord * 1000 / duration) + " / s");


        String startRowKey = "" + String.format("%.8f", 0.0) + "-" + String.format("%08d", 0);

        String endRowKey = "" + String.format("%.8f", 0.1) + "-" + String.format("%08d", Long.MAX_VALUE);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("zcode");
        columnNames.add("payload");
        columnNames.add("timestamp");

        long start = System.currentTimeMillis();
        hBaseHandler.search(table, columnFamilyName, columnNames, Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));
        System.out.println(System.currentTimeMillis() - start);
    }
}
