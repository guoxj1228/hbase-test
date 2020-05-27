package com.xj

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbaseTest {

  def kerberosSecurity(): Unit ={
    System.setProperty("java.security.krb5.conf","E:\\workSpace\\kerbors\\krb5.conf")
    val user = "hbase@LAWYEE.COM"
    val keyPath = "E:\\workSpace\\kerbors\\hbase.keytab"

    try{
      UserGroupInformation.loginUserFromKeytab(user, keyPath)
      println("认证： "+UserGroupInformation.isSecurityEnabled)
    }
  }

  def main(args: Array[String]): Unit = {
    kerberosSecurity()
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","192.168.0.231,192.168.0.232,192.168.0.233")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    UserGroupInformation.setConfiguration(hbaseConf)

    val hbaseContext = new HBaseContext(sc,hbaseConf)
    val hbaseRdd = hbaseContext.hbaseRDD(TableName.valueOf("test04"),new Scan())
    println(hbaseRdd.count())

  }

}
