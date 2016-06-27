package com.dream.hiter.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
 * http://www.cnblogs.com/ylqmf/archive/2012/02/18/2357537.html
 * http://www.cnblogs.com/ggjucheng/p/3380267.html
 * 
 * @author GIT
 *
 */
public class HbaseUtil {

  private static Configuration configuration;
  private static final Logger logger = Logger.getLogger(HbaseUtil.class);

  static {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("hbase.zookeeper.quorum", "192.168.1.114");
    configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
    configuration.set("hbase.master.info.bindAddress", "192.168.1.114:16000");
  }

  /**
   * Create a table in hbase.
   * 
   * @param tableName
   * @param family
   * @throws Exception
   */
  public void createTable(String tableName, String[] family) throws Exception {
    logger.info("Create hbase table :" + tableName);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Admin admin = connection.getAdmin();
    // admin.createNamespace(NamespaceDescriptor.create("").build()); //create namespace
    if (admin.tableExists(TableName.valueOf(tableName))) {
      logger.warn("Create hbase table" + tableName + " is fail,becase it exists");
    } else {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
      for (int i = 0; i < family.length; i++) {
        desc.addFamily(new HColumnDescriptor(family[i]));
      }
      logger.info("Create hbase table " + tableName + " success");
    }
  }

  /**
   * Drop a table from hbase.
   * 
   * @param tableName:the name of table in hbase
   * @throws Exception
   */
  public void dropTable(String tableName) throws Exception {
    logger.info("Drop hbase table :" + tableName);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Admin admin = connection.getAdmin();
    if (admin.tableExists(TableName.valueOf(tableName))) {
      logger.warn("Drop hbase table" + tableName + " is fail,becase it not exists");
    } else {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
      logger.info("Drop hbase table " + tableName + " success");
    }
  }

  /**
   * 
   * @param tableName
   * @throws IOException
   */
  public void insertRowData(String tableName, String rowData, Map<String, String> data)
      throws IOException {
    logger.info("Insert a row of data into hbase table :" + tableName);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf(tableName));
    Put put = new Put(rowData.getBytes());// Represent a row data
    for (String key : data.keySet()) {
      put.addColumn(key.getBytes(), null, data.get(key).getBytes());
    }
    table.put(put);
  }

  /**
   * Delete a row data.
   * 
   * @param tableName
   * @param rowKey
   * @throws IOException
   */
  public void deleteRow(String tableName, String rowKey) throws IOException {
    logger.info("Delete a row of data in hbase table :" + tableName + " rowkey:" + rowKey);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf(tableName));
    Delete delete = new Delete(rowKey.getBytes());
    table.delete(delete);
  }


  public void QueryAll(String tableName) throws IOException {
    logger.info("Query all data in hbase table :" + tableName);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf(tableName));
    ResultScanner rs = table.getScanner(new Scan());
    for (Result r : rs) {
      for (Cell cell : r.rawCells()) {
        String rowKey = CellUtil.getCellKeyAsString(cell);
        String row = CellUtil.cloneRow(cell).toString();
        System.out.println(row);
      }
    }
  }

  public void QueryByCondition(String tableName, String rowKey) throws IOException {
    logger.info("Query all data in hbase table by condition:" + tableName);
    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(rowKey.getBytes());//
    Result r = table.get(get);
    for (Cell cell : r.rawCells()) {
      String rowData = CellUtil.getCellKeyAsString(cell);
      String row = CellUtil.cloneRow(cell).toString();
      System.out.println(row);
    }
  }

  public static void main(String[] args) throws Exception {
    new HbaseUtil().createTable("zhang",new String[]{"one","two"});
  }

}
