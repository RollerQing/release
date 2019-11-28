package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

class DMReleaseCustomer{

}


/**
  * 投放目标客户数据集市
  */
object DMReleaseCustomer {

  val logger :Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)






  /**
    * 目标客户
    * status=01
    */
  def handleReleaseJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val saveMode:SaveMode = SaveMode.Overwrite
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL


      //日志数据
      val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //当天数据
      val customerCondition = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER)
        .where(customerCondition)
        .selectExpr(customerColumns:_*)
        .persist(storageLevel)
      //println(s"customerReleaseDF================")
      //customerReleaseDF.where("sources='baidu'").show(100,false)



      //渠道统计
      val customerSourceGroupColumns : Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",$"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")
      val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourcesColumns()


      //聚合
      val customerSourceDMDF = customerReleaseDF
        .groupBy(customerSourceGroupColumns:_*)
        .agg(
          countDistinct("device_num").alias(s"${ReleaseConstant.COL_MEASURE_USER_COUNT}"),
          count("release_session").alias(s"${ReleaseConstant.COL_MEASURE_TOTAL_COUNT}")
        )
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
        .selectExpr(customerSourceColumns:_*)



      customerSourceDMDF.sort($"sources".asc).show(100, false)
      //SparkHelper.writeTableData(customerSourceDMDF, ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES, saveMode)


      //多维统计
//      val customerCubeGroupColumns : Seq[Column] = Seq[Column](
//        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
//          $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
//          $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
//        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
//        $"${ReleaseConstant.COL_RELEASE_GENDER}",
//        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
//      )
//      val customerCubeColumns = DMReleaseColumnsHelper.selectDMCustomerCubeColumns()
//
//      val customerCubeDF = customerReleaseDF
//        .cube(customerCubeGroupColumns:_*)
//        .agg(
//          countDistinct(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_MEASURE_USER_COUNT}"),
//          count(lit(ReleaseConstant.COL_RELEASE_SESSION_ID)).alias(s"${ReleaseConstant.COL_MEASURE_TOTAL_COUNT}")
//        )
//        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
//        .selectExpr(customerCubeColumns:_*)
//      SparkHelper.writeTableData(customerCubeDF, ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE, saveMode)




    }catch{
      case ex:Exception => {
        println(s"DMReleaseCustomer.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DMReleaseCustomer.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }



  /**
    * 投放目标客户
    * @param appName
    */
  def handleJobs(appName :String, bdp_day_begin:String, bdp_day_end:String) :Unit = {
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
        .setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSpark(sconf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }

    }catch{
      case ex:Exception => {
        println(s"DMReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    //val Array(appName, bdp_day_begin, bdp_day_end) = args

        val appName: String = "dw_shop_log_job"
        val bdp_day_begin:String = "20190613"
        val bdp_day_end:String = "20190613"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}
