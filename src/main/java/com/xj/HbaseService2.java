package com.xj;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class HbaseService2 {
    private String quorum = "";

    private Configuration conf = HBaseConfiguration.create();

    private Timer timer = null;

    private Connection connection;

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    private static Logger log = Logger.getLogger(HbaseService2.class);

    public void init() {
        this.conf.set("hbase.zookeeper.quorum", this.quorum);
        this.conf.set("hbase.zookeeper.property.clientport", "2181");
        this.conf.setLong("hbase.client.scanner.timeout.period", 1200L);
        this.conf.setLong("hbase.rpc.timeout", 1200L);
        this.conf.setLong("hbase.client.fast.fail.cleanup.duration", 1200L);
        this.conf.setLong("mapreduce.task.timeout", 1200L);
        try {
            this.connection = ConnectionFactory.createConnection(this.conf);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public boolean isExistRowKey(String rowkey, String tableName) {
        HTable table = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey.getBytes());
            boolean flag = table.exists(get);
            if (!flag)
                return false;
        } catch (IOException e) {
            log.info(e.getMessage());
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                log.info(e.getMessage());
            }
        }
        return true;
    }

    public boolean insertRow(String tableName, Map<String, String> rowData) {
        List<Map<String, String>> rowDatas = new ArrayList<Map<String, String>>();
        rowDatas.add(rowData);
        insertRows(tableName, rowDatas);
        return true;
    }

    public boolean insertRows(String tableName, List<Map<String, String>> rowDatas) {
        boolean flag = false;
        List<Put> listPut = new ArrayList<Put>();
        for (Map<String, String> rowData : rowDatas) {
            String rowkey = rowData.get("rowkey");
            long version = 0L;
            if (rowkey == null)
                rowkey = UUID.randomUUID().toString();
            Put put = new Put(Bytes.toBytes(rowkey));
            for (String key : rowData.keySet()) {
                if (key.equals("rowkey"))
                    continue;
                String[] split = key.split(":");
                if (split.length == 3) {
                    version = Long.parseLong(split[2]);
                    put.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]), version,
                            Bytes.toBytes(rowData.get(key)));
                    continue;
                }
                put.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]), Bytes.toBytes(rowData.get(key)));
            }
            listPut.add(put);
        }
        HTable table = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            table.put(listPut);
            flag = true;
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
        return flag;
    }

    public void deleteRow(String tableName, String rowkey) {
        List<String> rowkeys = new ArrayList<String>();
        rowkeys.add(rowkey);
        deleteRows(tableName, rowkeys);
    }

    public void deleteRows(String tableName, List<String> rowkeys) {
        List<Delete> listDelete = new ArrayList<Delete>();
        for (String rowkey : rowkeys) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            listDelete.add(delete);
        }
        HTable table = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            table.delete(listDelete);
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
        }
    }

    public void deleteColumns(String tableName, List<String> rowkeys, String columnName) {
        List<Delete> listDelete = new ArrayList<Delete>();
        String[] split = columnName.split(":");
        for (String rowkey : rowkeys) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumns(split[0].getBytes(), split[1].getBytes());
            listDelete.add(delete);
        }
        HTable table = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            table.delete(listDelete);
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
        }
    }

    public void close() {
        try {
            this.connection.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        if (this.timer != null)
            this.timer.cancel();
    }

    public List<Map<String, String>> queryByRowkeys(String tableName, List<String> rowKeys, String[] columnNames) {
        return queryByRowkeys(tableName, rowKeys, columnNames, -1L);
    }

    public List<Map<String, String>> queryByRowkeys(String tableName, List<String> rowKeys, String[] columnNames, long version) {
        HTable table = null;
        ArrayList<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            List<Get> listGet = new ArrayList<Get>();
            for (String rowkey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowkey));
                if (version != -1L)
                    get.setTimeStamp(version);
                byte b1;
                int j;
                String[] arrayOfString;
                for (j = (arrayOfString = columnNames).length, b1 = 0; b1 < j; ) {
                    String columnName = arrayOfString[b1];
                    String[] split = columnName.split(":");
                    get.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
                    b1++;
                }
                listGet.add(get);
            }
            Result[] results = table.get(listGet);
            byte b;
            int i;
            Result[] arrayOfResult1;
            for (i = (arrayOfResult1 = results).length, b = 0; b < i; ) {
                Result result = arrayOfResult1[b];
                Map<String, String> mapResult = new HashMap<String, String>();
                mapResult.put("rowkey", Bytes.toString(result.getRow()));
                byte b1;
                int j;
                String[] arrayOfString;
                for (j = (arrayOfString = columnNames).length, b1 = 0; b1 < j; ) {
                    String columnName = arrayOfString[b1];
                    String[] split = columnName.split(":");
                    Cell cell = result.getColumnLatestCell(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
                    if (cell != null)
                        mapResult.put(columnName, Bytes.toString(CellUtil.cloneValue(cell)));
                    b1++;
                }
                resultList.add(mapResult);
                b++;
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
        }
        return resultList;
    }

    public List<Map<String, String>> queryByRowkeys(String tableName, List<String> rowKeys, String[] qualifiers, String family) {
        HTable table = null;
        ArrayList<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            List<Get> listGet = new ArrayList<Get>();
            for (String rowkey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowkey));
                byte b1;
                int j;
                String[] arrayOfString;
                for (j = (arrayOfString = qualifiers).length, b1 = 0; b1 < j; ) {
                    String qualifier = arrayOfString[b1];
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    b1++;
                }
                listGet.add(get);
            }
            Result[] results = table.get(listGet);
            byte b;
            int i;
            Result[] arrayOfResult1;
            for (i = (arrayOfResult1 = results).length, b = 0; b < i; ) {
                Result result = arrayOfResult1[b];
                Map<String, String> mapResult = new HashMap<String, String>();
                mapResult.put("rowkey", Bytes.toString(result.getRow()));
                byte b1;
                int j;
                String[] arrayOfString;
                for (j = (arrayOfString = qualifiers).length, b1 = 0; b1 < j; ) {
                    String qualifier = arrayOfString[b1];
                    Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                    if (cell != null)
                        mapResult.put(qualifier, Bytes.toString(CellUtil.cloneValue(cell)));
                    b1++;
                }
                resultList.add(mapResult);
                b++;
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
        }
        return resultList;
    }

    public List<Map<String, String>> queryVersionRowAll(String tableName, List<String> rowKeys, List<String> columnNames, int version) {
        HTable table = null;
        ArrayList<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        List<Get> listGet = new ArrayList<Get>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            for (String rowkey : rowKeys) {
                Get get = new Get(rowkey.getBytes());
                get.setMaxVersions(version);
                for (String columnName : columnNames) {
                    String[] split = columnName.split(":");
                    get.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
                }
                listGet.add(get);
            }
            Result[] rsAll = table.get(listGet);
            byte b;
            int i;
            Result[] arrayOfResult1;
            for (i = (arrayOfResult1 = rsAll).length, b = 0; b < i; ) {
                Result rs = arrayOfResult1[b];
                List<Cell> listCells = rs.listCells();
                String rowkey = new String(rs.getRow());
                List<List<String>> list = new ArrayList<List<String>>();
                for (String columnName : columnNames) {
                    String[] split = columnName.split(":");
                    List<String> listCell = new ArrayList<String>();
                    for (int v = 0; v < listCells.size(); v++) {
                        Cell cell = listCells.get(v);
                        String cellName = new String(CellUtil.cloneQualifier(cell));
                        String columValue = new String(CellUtil.cloneValue(cell));
                        if (split[1].equals(cellName)) {
                            listCell.add(cellName);
                            listCell.add(columValue);
                        }
                    }
                    if (listCell.size() % version * 2 != 0) {
                        int j = (version * 2 - listCell.size() % version * 2) / 2;
                        for (; j > 0; j--) {
                            listCell.add(" ");
                            listCell.add(" ");
                        }
                    }
                    list.add(listCell);
                    listCell = new ArrayList<String>();
                }
                Map<String, String> map = null;
                for (int v1 = 0; v1 < version * 2; v1++) {
                    map = new HashMap<String, String>();
                    map.put("rowkey", rowkey);
                    for (int n = 0; n < list.size(); n++) {
                        List<String> list2 = list.get(n);
                        String key = list2.get(v1);
                        String value = list2.get(v1 + 1);
                        map.put(key, value);
                    }
                    resultList.add(map);
                    v1++;
                }
                b++;
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
        }
        return resultList;
    }

    public long getCounter(String tableName, String rowKey, String columnName) {
        HTable table = null;
        long counter = 0L;
        String[] split = columnName.split(":");
        String column = split[1];
        String columnFamily = split[0];
        Get get = new Get(rowKey.getBytes());
        get.addColumn(columnFamily.getBytes(), column.getBytes());
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(columnFamily.getBytes(), column.getBytes());
            counter = Bytes.toLong(CellUtil.cloneValue(cell));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return counter;
    }

    public long increaseCounter(String tableName, String rowKey, String columnName, int step) {
        HTable table = null;
        long incrementColumnValue = 0L;
        String[] split = columnName.split(":");
        String column = split[1];
        String columnFamily = split[0];
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            incrementColumnValue = table.incrementColumnValue(rowKey.getBytes(), columnFamily.getBytes(),
                    column.getBytes(), step, Durability.SKIP_WAL);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return incrementColumnValue;
    }

    public ResultScanner queryByScan(String tableName, Scan scan) {
        HTable table = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        ResultScanner rss = null;
        try {
            rss = table.getScanner(scan);
        } catch (IOException e) {
            rss.close();
            log.error(e.getMessage());
        }
        return rss;
    }

    public List<Map<String, String>> queryByRowkeyRange(String tableName, String startRowKey, String endRowKey, List<String> columnNames) {
        Scan scan = new Scan();
        scan.setStartRow(startRowKey.getBytes());
        scan.setStopRow(endRowKey.getBytes());
        HTable table = null;
        ArrayList<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        ResultScanner rss = null;
        try {
            rss = table.getScanner(scan);
            for (Result rs : rss) {
                Map<String, String> map = new HashMap<String, String>();
                String rowkey = new String(rs.getRow());
                map.put("rowkey", rowkey);
                for (String columnName : columnNames) {
                    String[] split = columnName.split(":");
                    Cell cell = rs.getColumnLatestCell(split[0].getBytes(), split[1].getBytes());
                    String columStr = new String(CellUtil.cloneValue(cell));
                    map.put(columnName, columStr);
                }
                resultList.add(map);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            if (rss != null)
                rss.close();
        }
        return resultList;
    }

    public Map<String, String> getColumnValues(String tableName, String rowKey, String columnName) {
        HTable table = null;
        Map<String, String> map = new HashMap<String, String>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setMaxVersions();
        try {
            Result result = table.get(get);
            String[] split = columnName.split(":");
            List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
            String res = "";
            for (Cell cell : columnCells) {
                res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                map.put((new StringBuilder(String.valueOf(cell.getTimestamp()))).toString(), res);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return map;
    }

    public String getColumnValue(String tableName, String rowKey, String columnName) {
        HTable table = null;
        String value = null;
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        String[] split = columnName.split(":");
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        try {
            Result result = table.get(get);
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
            if (cell != null)
                value = Bytes.toString(CellUtil.cloneValue(cell));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return value;
    }

    public Map<String, String> getColumnsValue(String tableName, String rowKey, String[] columnNames) {
        return getColumnsValue(tableName, rowKey, columnNames, -1L);
    }

    public Map<String, String> getColumnsValue(String tableName, String rowKey, String[] columnNames, long version) {
        HTable table = null;
        Map<String, String> mapResult = new HashMap<String, String>();
        try {
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        if (version != -1L)
            try {
                get.setTimeStamp(version);
            } catch (IOException e) {
                e.printStackTrace();
            }
        for (int i = 0; i < columnNames.length; i++) {
            String[] split = columnNames[i].split(":");
            if (split.length == 2)
                get.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
        }
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte b;
        int j;
        String[] arrayOfString;
        for (j = (arrayOfString = columnNames).length, b = 0; b < j; ) {
            String columnName = arrayOfString[b];
            String[] split = columnName.split(":");
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
            if (cell != null)
                mapResult.put(columnName, Bytes.toString(CellUtil.cloneValue(cell)));
            b++;
        }
        return mapResult;
    }

    public void hMajorCompact(String tableName) {
        try {
            Admin hAdmin = this.connection.getAdmin();
            hAdmin.majorCompact(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public void deleteRowByVersion(String tableName, String rowkey, String columnName, long version) {
        HTable table = null;
        try {
            String[] split = columnName.split(":");
            table = (HTable)this.connection.getTable(TableName.valueOf(tableName));
            Delete detete = new Delete(rowkey.getBytes());
            detete.addColumn(split[0].getBytes(), split[1].getBytes(), version);
            table.delete(detete);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public void setKerboros(String krbPath, String keytabPath, String principal) {
        System.setProperty("java.security.krb5.conf", krbPath);
        this.conf.set("hadoop.security.authentication", "kerberos");
        this.conf.set("hbase.security.authentication", "kerberos");
        this.conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@LAWYEE.COM");
        UserGroupInformation.setConfiguration(this.conf);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            System.out.println(UserGroupInformation.getLoginUser().toString());
            System.out.println(UserGroupInformation.isSecurityEnabled());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        this.timer = new Timer(true);
        this.timer.schedule(new TimerTask() {
            public void run() {
                try {
                    UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                    HbaseService2.log.info(UserGroupInformation.getLoginUser().toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        },  0L, 3600000L);
    }

    public void setSecurity(String securityConfig) {
        String[] scs = securityConfig.split(";");
        setKerboros(scs[0], scs[1], scs[2]);
    }

    public void setParamMap(Map<String, String> paramMap) {
        setKerboros(paramMap.get("krbPath"), paramMap.get("keytabPath"), paramMap.get("principal"));
    }
}

