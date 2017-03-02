package indexingTopology.util;


import indexingTopology.DataSchema;
import indexingTopology.config.TopologyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
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

    public void creatTable(String tableName, String familyName, String indexField) throws Exception {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        if (indexField != null) {
        }

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

    public void addIntValue(Table table, String columnFamilyName, String columnName, byte[] rowKey, Integer data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void addDoubleValue(Table table, String columnFamilyName, String columnName, byte[] rowKey, Double data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void addLongValue(Table table, String columnFamilyName, String columnName, byte[] rowKey, Long data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }


    public void addStringValue(Table table, String columnFamilyName, String columnName, byte[] rowKey, String data, Put put) throws IOException {
//        TableName tableName = TableName.valueOf(name);

//        Put put = new Put(rowKey);
        put.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data));

//        Table table = connection.getTable(tableName);

//        table.put(put);
    }

    public void search(Table table, String columnFamilyName, List<String> columns, byte[] startRow, byte[] endRow) throws IOException {
//        TableName tableName = TableName.valueOf(name);
//        Table table = connection.getTable(tableName);

        Scan scan = new Scan(startRow, endRow);

        for (String columnName : columns) {
            scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
        }

//        PrefixFilter prefixFilter = new PrefixFilter(bytes);
//        scan.setFilter(prefixFilter);
//        ResultScanner resultScanner = table.getScanner(scan);

        ResultScanner scanner = table.getScanner(scan);
//        Result row = table.get(new Get(bytes));
        int count = 0;

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            ++count;
        }

//            System.out.println("Found row : " + result);
//            printRow(result);

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
