package com.qf.bigdata.release.syncdata

import java.util.Properties

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.{CommonUtil, PropertyUtil}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


/**
  * 数据源数据同步
  */

class SyncDataJob{

}


object SyncDataJob {

  val logger :Logger = LoggerFactory.getLogger(SyncDataJob.getClass)

  /**
    * 数据同步
    */
  def handleSyncData(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try {
      //mysql链接
      val mysqlConnUrlEnd = "?serverTimezone=UTC&characterEncoding=utf-8"
      val jdbcPro :Properties = PropertyUtil.readProperties(ReleaseConstant.JDBC_CONFIG_PATH)
      if(null != jdbcPro){
        val url = jdbcPro.getProperty(ReleaseConstant.JDBC_CONFIG_URL)
        val user = jdbcPro.getProperty(ReleaseConstant.JDBC_CONFIG_USER)
        val password = jdbcPro.getProperty(ReleaseConstant.JDBC_CONFIG_PASS)
        val serverUrl = url.substring(0, url.lastIndexOf("/")+1)

        val prop = new Properties()
        prop.put("user", user)
        prop.put("password", password)

        val syncTables :Array[Row] =  SparkHelper.getDBData(spark, url, ReleaseConstant.SYNCDATA_MYSQL_HIVE, user, password)
        if(!syncTables.isEmpty){
          for(dbTable <- syncTables if dbTable.getAs[Int]("status") == ReleaseConstant.SYNCDATA_USED){
            //println(s"table=>${dbTable}")
            try{
              //println(s"dbTable.schema=${dbTable.schema}")
              val dbMysql = dbTable.getAs[String]("db_mysql")
              val tableMysql = dbTable.getAs[String]("table_mysql")
              val dbHive = dbTable.getAs[String]("db_hive")
              val tableHive = dbTable.getAs[String]("table_hive")
              val status = dbTable.getAs[Int]("status")
              val saveMode = dbTable.getAs[String]("save_mode")
              val fieldMap = dbTable.getAs[String]("fieldMap")//1 需要转化 字段,字段

              val syncSourceUrl = serverUrl+dbMysql+mysqlConnUrlEnd
              val hiveTable = s"${dbHive}.${tableHive}"
              println(s"sync_source_url=${syncSourceUrl},hiveTable=$hiveTable")
              val syncSourceDF = spark.read.jdbc(syncSourceUrl,tableMysql,prop)

              //syncSourceDF.printSchema()
              syncSourceDF.repartition(ReleaseConstant.DEF_PARTITIONS_FACTOR).write.mode(saveMode).saveAsTable(hiveTable)

            }catch {
              case ex: Exception => {
                println(s"SyncDataJob.handleSyncData  occur exception：app=[$appName], msg=$ex")
                logger.error(ex.getMessage, ex)
              }
            }

          }
        }
      }else{
        logger.error("SyncDataJob.handleSyncData source error!")
      }


    } catch {
      case ex: Exception => {
        println(s"SyncDataJob.handleSyncData occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    } finally {
      println(s"SyncDataJob.handleSyncData End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  /**
    * 行为日志
    * @param appName
    */
  def handleJobs(appName :String, bdp_day_begin:String, bdp_day_end:String, range:Int) :Unit = {
    var spark :SparkSession = null
    try{
      //spark配置参数
      val sconf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
      //.setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSpark(sconf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleSyncData(spark, appName, bdp_date)
      }

    }catch{
      case ex:Exception => {
        println(s"SyncDataJob.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }




  def main(args: Array[String]): Unit = {

    val Array(appName, bdp_day_begin, bdp_day_end) = args

    //    val appName: String = "dw_shop_log_job"
    //    val bdp_day_begin:String = "20190525"
    //    val bdp_day_end:String = "20190525"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end, ReleaseConstant.RANGE_DAYS)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}
