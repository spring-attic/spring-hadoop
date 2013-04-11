package org.springframework.data.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

import static org.junit.Assert.*;

/**
 * @author Muhammad Ashraf
 * @since 4/10/13
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HBaseRemoteTest {

    @Autowired
    HbaseTemplate template;

    @Resource(name = "hbaseConfiguration")
    Configuration config;

    String tableName = "myRemoteTable";
    String columnName = "myColumnFamily";
    String rowName = "myLittleRow";
    String qualifier = "someQualifier";
    String value = "Some Value";

    @Test
    public void testHBaseConnection() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(tableName)) {
            System.out.println("deleting table...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor(columnName));
        assertTrue(tableDescriptor.hasFamily(Bytes.toBytes(columnName)));
        admin.createTable(tableDescriptor);

        // block A
        System.out.println("Created table...");
        HTable table = new HTable(config, tableName);

        Put p = new Put(Bytes.toBytes(rowName));
        p.add(Bytes.toBytes(columnName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(p);

        System.out.println("Doing put..");
        Get g = new Get(Bytes.toBytes(rowName));
        Result r = table.get(g);
        byte[] val = r.getValue(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));

        assertEquals(value, Bytes.toString(val));
        System.out.println("Doing get..");


        // block B
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));
        ResultScanner scanner = table.getScanner(s);

        try {
            // Scanners return Result instances.
            for (Result rr : scanner) {
                System.out.println("Found row: " + rr);
            }
        } catch (Exception ex) {
            System.out.println("Caught exception " + ex);
        } finally {
            // Make sure you close your scanners when you are done!
            // Thats why we have it inside a try/finally clause
            scanner.close();
        }
    }


    @Test
    public void testTemplate() throws Exception {

        template.execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                Put p = new Put(Bytes.toBytes(rowName));
                p.add(Bytes.toBytes(columnName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                table.put(p);

                System.out.println("Doing put..");
                Get g = new Get(Bytes.toBytes(rowName));
                Result r = table.get(g);
                byte[] val = r.getValue(Bytes.toBytes(columnName), Bytes.toBytes(qualifier));

                assertEquals(value, Bytes.toString(val));
                return null;
            }
        });



        // equivalent of block B
        System.out.println("Found rows " + template.find(tableName, columnName, qualifier, new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int rowNum) throws Exception {
                return result.toString();
            }
        }));

    }
}
