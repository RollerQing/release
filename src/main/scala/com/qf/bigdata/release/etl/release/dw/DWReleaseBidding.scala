package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.{BiddingStatusEnum, BiddingTypeEnum, ReleaseStatusEnum}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}


class DWReleaseBidding{

}

/**
  * 投放目标客户
  */
object DWReleaseBidding {

  val logger :Logger = LoggerFactory.getLogger(DWReleaseBidding.getClass)


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
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode:SaveMode = SaveMode.Overwrite

      //竞价列数据
      val odsBiddingColumns = DWReleaseColumnsHelper.selectODSReleaseBiddingColumns()

      //广告投放数据（竞价）
      val biddingReleaseCondition = ( col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day) and col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.BIDING.getCode))
      val biddingReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, odsBiddingColumns)
        .where(biddingReleaseCondition)
        .persist(storageLevel)
      println(s"biddingReleaseDF================")
      biddingReleaseDF.show(10, false)

      //pmp固定价格
      val pmpBiddingColumns = DWReleaseColumnsHelper.selectDWReleasePMPBiddingColumns()
      val pmpBiddingCondition = col(s"${ReleaseConstant.COL_RELEASE_BIDDING_TYPE}") === lit(BiddingTypeEnum.PMP.getCode)
      val pmpBiddingDF = biddingReleaseDF.where(pmpBiddingCondition)
          .selectExpr(pmpBiddingColumns:_*)
      println(s"pmpBiddingDF================")
      pmpBiddingDF.show(10, false)


      //rtb实时竞价
      val rtbBiddingCondition = col(s"${ReleaseConstant.COL_RELEASE_BIDDING_TYPE}") === lit(BiddingTypeEnum.RTB.getCode)
      val rtbBiddingDF = biddingReleaseDF.where(rtbBiddingCondition).persist(storageLevel)
      println(s"rtbBiddingDF================")
      rtbBiddingDF.show(10, false)


      //出价
      val bidPriceCondition = col(s"${ReleaseConstant.COL_RELEASE_BIDDING_STATUS}") === lit(BiddingStatusEnum.BID_PRICE.getCode)
      val bidPriceDF = rtbBiddingDF.where(bidPriceCondition)

      //获胜价格
      val winPriceCondition = col(s"${ReleaseConstant.COL_RELEASE_BIDDING_STATUS}") === lit(BiddingStatusEnum.WIN_PRICE.getCode)
      val winPriceDF = rtbBiddingDF.where(winPriceCondition)

      //竞价组合
      val nrtbBiddingColumns = DWReleaseColumnsHelper.selectDWReleaseBiddingColumns()
      val nrtbBiddingDF = bidPriceDF.alias("b")
        .join(winPriceDF.alias("w"), bidPriceDF("release_session") === winPriceDF("release_session"), "left")
        .selectExpr(nrtbBiddingColumns:_*)
      println(s"nrtbBiddingDF================")
      nrtbBiddingDF.show(10,false)

      //PMP+RTB
      val biddingDF = nrtbBiddingDF.union(pmpBiddingDF)
      println(s"biddingDF================")
      biddingDF.show(10,false)


      //竞价表
      //SparkHelper.writeTableData(biddingDF, ReleaseConstant.DW_RELEASE_BIDDING, saveMode)


      //去缓存处理
      biddingReleaseDF.unpersist()
      rtbBiddingDF.unpersist()

    }catch{
      case ex:Exception => {
        println(s"DWReleaseCustomer.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DWReleaseCustomer.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
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
        //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
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
        println(s"DWReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
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

    val appName: String = "dw_release_customer_job"
    val bdp_day_begin:String = "20190613"
    val bdp_day_end:String = "20190613"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}


