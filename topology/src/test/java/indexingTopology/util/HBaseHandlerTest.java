package indexingTopology.util;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 27/2/17.
 */
public class HBaseHandlerTest {
    /*
    @Test
    public void search() throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();

        List<String> columns = new ArrayList<>();
        columns.add("a1");
        columns.add("a2");
        columns.add("a3");

        Connection connection = hBaseHandler.getConnection();

        Table table = connection.getTable(TableName.valueOf("MyTable"));

        hBaseHandler.search(table, "TestColumnFamily", columns, Bytes.toBytes("0.1-1"), Bytes.toBytes("0.1-4"));
    }

    @Test
    public void addIntValue() throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();

        Connection connection = hBaseHandler.getConnection();

        Table table = connection.getTable(TableName.valueOf("MyTable"));

//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a1", Bytes.toBytes("0.1-1603-1"), 1);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a2", Bytes.toBytes("0.1-1603-1"), 2);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a3", Bytes.toBytes("0.1-1603-1"), 3);
//
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a1", Bytes.toBytes("0.1-400-2"), 1);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a2", Bytes.toBytes("0.1-400-2"), 2);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a3", Bytes.toBytes("0.1-400-2"), 3);
//
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a1", Bytes.toBytes("0.1-23-3"), 1);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a2", Bytes.toBytes("0.1-23-3"), 2);
//        hBaseHandler.addIntValue(table, "TestColumnFamily", "a3", Bytes.toBytes("0.1-23-3"), 3);
    }

    @Test
    public void addColumnFamily() throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();

        hBaseHandler.addColumnFamily("MyTable", "NewColumnFamily");

        assertEquals(2, hBaseHandler.getNumberOfColumnFamilies("MyTable"));
    }


    @Test
    public void createTable() throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();

        hBaseHandler.creatTable("MyTable", "TestColumnFamily", null);

        TableName tableName = TableName.valueOf("MyTable");

        Admin admin = hBaseHandler.getConnection().getAdmin();
        HTableDescriptor table = admin.getTableDescriptor(tableName);

        assertEquals(true, admin.tableExists(table.getTableName()));
    }
    */
}