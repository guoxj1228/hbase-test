package com.xj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

public class HbaseService {
    Logger logger = Logger.getLogger(HbaseService.class);
    Configuration conf = HBaseConfiguration.create();
    Connection connection =null;
    String keytabPath="";
    String zkHost="";
    String krb5Parh="";
    String principal="";

    public void setZkHost(String zkHost) {
        this.zkHost = zkHost;
    }
    public void setKrb5Parh(String krb5Parh) {
        this.krb5Parh = krb5Parh;
    }
    public void setPrincipal(String principal) {
        this.principal = principal;
    }
    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }


    public void init(){
        conf.set("hbase.zookeeper.quorum",zkHost);
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,12000);
        conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY,12000);
        conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS,12000);
        conf.setLong("mapreduce.task.timeout",12000);
        try {
            connection = ConnectionFactory.createConnection(conf);
            System.out.println(connection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在kerberos环境下，进行hbase的安全认证登录
     */
    public void setHbaseKerberos(){
        System.setProperty("java.security.krb5.conf",krb5Parh);
        conf.set("hadoop.security.authentication","kerberos");
        conf.set("hbase.security.authorization","kerberos");
        conf.set("hbase.master.kerberos.principal","hbase/_HOST@LAWYEE.COM");
        conf.set("hbase.regionserver.kerberos.principal","hbase/_HOST@LAWYEE.COM");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(principal,keytabPath);
            boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
            System.out.println(securityEnabled);
            System.out.println(UserGroupInformation.getLoginUser());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void readByRowkey(){

    }

    public void writeHbase(String tableName){
        try {
            HTable table = (HTable)connection.getTable(TableName.valueOf(tableName));
            ArrayList<Put> puts = new ArrayList<Put>();
            Put put =null;
            for(int count=0;count<1000;count++){
            for(int i=0;i<1;i++){
                String rowkey= UUID.randomUUID().toString();
                put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("c1"), Bytes.toBytes("你因犯滥用职权罪一案，不服江苏省如皋市人民法院（2002）皋刑初字第200号刑事判决及（2006）皋刑监字第001号驳回申诉通知，江苏省南通市中级人民法院（2008）通中刑监字第0012号驳回申诉通知，江苏省高级人民法院（2011）苏刑监字第305号驳回申诉通知，向本院提出申诉，要求重新审判。"));
                puts.add(put);
            }
            table.put(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //协处理器


    /**
     * 关闭hbase 连接
     */
    public void closeConnect(){
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
