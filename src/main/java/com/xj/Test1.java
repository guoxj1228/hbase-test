package com.xj;


import java.util.HashMap;
import java.util.Map;

public class Test1 {

    public static void main(String[] args) {
//        testWriteHbase();
        testInsertHbase();
    }
    public static void testWriteHbase(){
        HbaseService hbaseService = new HbaseService();
        hbaseService.setZkHost("192.168.0.231,192.168.0.232,192.168.0.233");
        hbaseService.setPrincipal("hbase@LAWYEE.COM");
        hbaseService.setKeytabPath("E:\\workSpace\\kerbors\\hbase.keytab");
        hbaseService.setKrb5Parh("E:\\workSpace\\kerbors\\krb5.conf");
        hbaseService.setHbaseKerberos();
        hbaseService.init();
        hbaseService.writeHbase("test04");
        hbaseService.closeConnect();
    }

    public static void testInsertHbase(){
        HbaseService2 hbaseService = new HbaseService2();
        hbaseService.setQuorum("192.168.0.231,192.168.0.232,192.168.0.233");
        hbaseService.setKerboros("E:\\workSpace\\kerbors\\krb5.conf","E:\\workSpace\\kerbors\\hbase.keytab","hbase@LAWYEE.COM");
        hbaseService.init();
//        System.out.println(hbaseService.getCounter("test04","001","c:c1"));
        Map<String, String> data = new HashMap<String, String>();
        data.put("rowkey","2");
        data.put("c:name","xiaoming");
        hbaseService.insertRow("test04",data);
    }
}
