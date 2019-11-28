package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

class DMReleaseExposure{

}


/**
  * 曝光主题数据集市
  */
object DMReleaseExposure {

  val logger :Logger = LoggerFactory.getLogger(DMReleaseExposure.getClass)

  /**
    * 曝光
    * status=03
    */
  def handleReleaseJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import org.apache.spark.sql.functions._
      import spark.implicits._
      //缓存级别
      val saveMode:SaveMode = SaveMode.Overwrite
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

      //日志数据
      val exposureColumns = DMReleaseColumnsHelper.selectDWReleaseExposureColumns()

      //当天dw中的曝光数据
      val exposureCondition = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)
      val exposureReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_EXPOSURE)
        .where(exposureCondition)
        .selectExpr(exposureColumns:_*)
        .persist(storageLevel)
      println(s"exposureReleaseDF================")
      exposureReleaseDF.show(10,false)

      //当天dm中的目标客户数据
      val customerDMColumns = DMReleaseColumnsHelper.selectDMCustomerSourcesColumns()
      val customerCondition = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)
      val customerDMReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES)
        .where(customerCondition)
        .selectExpr(customerDMColumns:_*)
        .persist(storageLevel)
      println(s"customerDMReleaseDF================")
      customerDMReleaseDF.show(10,false)

      //渠道统计|设备类型
      val exposureSourceGroupColumns : Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",$"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")
      val exposureSourceColumns = DMReleaseColumnsHelper.selectDMReleaseExposureColumns()

      //定义连接字段
      val joinColumnSeq = Seq(ReleaseConstant.COL_RELEASE_SOURCES,
        ReleaseConstant.COL_RELEASE_CHANNELS,ReleaseConstant.COL_RELEASE_DEVICE_TYPE)

      //先连接customerDMReleaseDF，在groupby
      val exposureSourceDMDF = exposureReleaseDF
          .join(customerDMReleaseDF,joinColumnSeq,"inner")
        .groupBy(exposureSourceGroupColumns:_*)
        .agg(
          countDistinct(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_MEASURE_EXPOSURE_COUNT}"),
          (count(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM))/sum(ReleaseConstant.COL_MEASURE_USER_COUNT)).alias(s"${ReleaseConstant.COL_MEASURE_EXPOSURE_RATES}")
        )
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
        .selectExpr(exposureSourceColumns:_*)
      //写表
      SparkHelper.writeTableData(exposureSourceDMDF, ReleaseConstant.DM_RELEASE_EXPOSURE_SOURCES, saveMode)


      //多维统计
      val exposureCubeGroupColumns : Seq[Column] = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
          $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
          $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
        $"${ReleaseConstant.COL_RELEASE_GENDER}",
        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
      )
      val exposureCubeColumns = DMReleaseColumnsHelper.selectDMExposureCubeColumns()

      val exposureCubeDF = exposureReleaseDF
        .join(customerDMReleaseDF,joinColumnSeq,"inner")
        .cube(exposureCubeGroupColumns:_*)
        .agg(
          countDistinct(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_MEASURE_EXPOSURE_COUNT}"),
          (count(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM))/sum(ReleaseConstant.COL_MEASURE_USER_COUNT)).alias(s"${ReleaseConstant.COL_MEASURE_EXPOSURE_RATES}")
        )
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
        .selectExpr(exposureCubeColumns:_*)
      SparkHelper.writeTableData(exposureCubeDF, ReleaseConstant.DM_RELEASE_EXPOSURE_CUBE, saveMode)




    }catch{
      case ex:Exception => {
        println(s"DMReleaseExposure.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DMReleaseExposure.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }



  /**
    * 曝光
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
      //.setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSpark(sconf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }

    }catch{
      case ex:Exception => {
        println(s"DMReleaseExposure.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
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

    //    val appName: String = "dm_release_exposuer_job"
    //    val bdp_day_begin:String = "20190525"
    //    val bdp_day_end:String = "20190525"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }

}
