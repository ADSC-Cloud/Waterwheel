package indexingTopology.util;


import com.esotericsoftware.kryo.io.Input;
import indexingTopology.config.TopologyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by acelzj on 27/2/17.
 */
public class HBaseHandler {

    Connection connection;

    Configuration configuration;

    Admin admin;

    public HBaseHandler() throws IOException {
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", TopologyConfig.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM);
        configuration.setInt("hbase.zookeeper.property.clientPort", TopologyConfig.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT);

        configuration.set("hbase.master", TopologyConfig.HBASE_MASTER);
        configuration.addResource(new Path(TopologyConfig.HBASE_CONF_DIR, "hbase-site.xml"));

        connection = ConnectionFactory.createConnection(configuration);

        admin = connection.getAdmin();
    }

    public void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }

        admin.createTable(table);
    }

    public void createTable(String tableName, String familyName, String indexField) throws Exception {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        table.setRegionReplication(1);


        table.addFamily(new HColumnDescriptor(familyName));

        createOrOverwrite(admin, table);
    }


    public void addColumnFamily(String name, String columnName) throws IOException {
        TableName tableName = TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exist.");
            System.exit(-1);
        }

        HTableDescriptor table = admin.getTableDescriptor(tableName);

        if (table.hasFamily(columnName.getBytes())) {
            System.out.println("Column family has been existed.");
            System.exit(-1);
        }

        HColumnDescriptor newColumn = new HColumnDescriptor(columnName);
        newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        admin.addColumn(tableName, newColumn);
    }


    public Connection getConnection() {
        return connection;
    }

    public int getNumberOfColumnFamilies(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);

        HTableDescriptor table = admin.getTableDescriptor(tableName);

        return table.getFamilies().size();
    }

    public void addIntValue(String columnFamilyName, String columnName, byte[] rowKey, Integer data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void addDoubleValue(String columnFamilyName, String columnName, byte[] rowKey, Double data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void addLongValue(String columnFamilyName, String columnName, byte[] rowKey, Long data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }


    public void addStringValue(String columnFamilyName, String columnName, byte[] rowKey, String data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void search(Table table, String columnFamilyName, List<String> columns, byte[] startRow, byte[] endRow, Long startTimestamp, Long endTimestamp, String columnValue) throws IOException {
//        TableName tableName = TableName.valueOf(name);
//        Table table = connection.getTable(tableName);

        Scan scan = new Scan(startRow, endRow);

        List<Filter> listForFilters = new ArrayList<Filter>();

        listForFilters.add(new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnValue), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startTimestamp)));
        listForFilters.add(new SingleColumnValueFilter(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnValue), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endTimestamp)));

        Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                listForFilters);

        for (String columnName : columns) {
            scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
        }

//        Filter filter = new ColumnRangeFilter(Bytes.toBytes(startTimestamp), true,
//                Bytes.toBytes(endTimestamp), true);

        scan.setFilter(filterList);
//        PrefixFilter prefixFilter = new PrefixFilter(bytes);
//        scan.setFilter(prefixFilter);
//        ResultScanner resultScanner = table.getScanner(scan);

        ResultScanner scanner = table.getScanner(scan);
//        Result row = table.get(new Get(bytes));
        int count = 0;


        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            ++count;
//
//            int i = 0;
//            String s = "";
//
//            if ( result.containsColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("zcode")) ) {
//                int destIp = Bytes.toInt(result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("zcode")));
//                System.out.println("Value: " + destIp + " ");
//            }
//
//            if ( result.containsColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("timestamp")) ) {
//                long timestamp = Bytes.toLong(result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("timestamp")));
//                System.out.println("Value: " + timestamp + " ");
//            }

        }

        System.out.println("*******" + count + "******");

        scanner.close();
    }

    public static void printRow(Result result) {
        String returnString = "";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("a1"), Bytes.toBytes("id"))) + ", ";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("a2"), Bytes.toBytes("zcode"))) + ", ";
        returnString += Bytes.toString(result.getValue(Bytes.toBytes("a3"), Bytes.toBytes("payload")));
        System.out.println(returnString);
    }
}
