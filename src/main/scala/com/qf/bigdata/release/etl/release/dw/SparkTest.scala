package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.release.dm.DMReleaseColumnsHelper
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, RowFactory, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}


object SparkTest {

  val logger :Logger = LoggerFactory.getLogger(SparkTest.getClass)

  /**
    * 目标客户
    * status=01
    */
  def handleDMJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
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
      println(s"customerReleaseDF================")
      customerReleaseDF.where("sources='baidu'").show(100,false)


      //渠道统计
      val customerSourceGroupColumns : Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",$"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")
      val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourcesColumns()


      val customerSourceDMDF = customerReleaseDF
        .groupBy(customerSourceGroupColumns:_*)
        .agg(
          countDistinct(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(s"${ReleaseConstant.COL_MEASURE_USER_COUNT}"),
          count(lit(ReleaseConstant.COL_RELEASE_SESSION_ID)).alias(s"${ReleaseConstant.COL_MEASURE_TOTAL_COUNT}")
        )
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
        .selectExpr(customerSourceColumns:_*)

      customerSourceDMDF.show(100, false)
      SparkHelper.writeTableData(customerSourceDMDF, ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES, saveMode)


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
    * 目标客户
    * status=01
    */
  def handleDWJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode:SaveMode = SaveMode.Overwrite

      //日志数据
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //当天数据
      val customerReleaseCondition = ( col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day) and col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode))
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, customerColumns)
        .where(customerReleaseCondition)
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      println(s"customerReleaseDF================")

      customerReleaseDF.show(10,false)

      //目标客户
      SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode)

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
    * 目标客户
    * status=01
    */
  def handleJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode:SaveMode = SaveMode.Overwrite

      //日志数据
      //全部数据 当天数据
      val begin = System.currentTimeMillis()
      val condition = col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
      val df = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION)
                          .where(condition)
                          //.persist(storageLevel)
      println(s"df================")
      //df.show(10,false)

      //目标客户
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      val customerReleaseCondition = col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode)
      val customerReleaseDF = df.selectExpr(customerColumns:_*)
        .where(customerReleaseCondition)
      val customerBegin = System.currentTimeMillis()
      println(s"customerReleaseDF========${customerBegin-begin}========")
      customerReleaseDF.show(10,false)
      //目标客户
      //SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode)

      //曝光
      val showColumns = DWReleaseColumnsHelper.selectDWReleaseShowColumns()
      val showReleaseCondition = col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.SHOW.getCode)
      val showReleaseDF = df.selectExpr(showColumns:_*)
        .where(showReleaseCondition)

      val showBegin = System.currentTimeMillis()
      println(s"showReleaseDF========${showBegin-begin}========")
      //showReleaseDF.show(10,false)

      //曝光
      SparkHelper.writeTableData(showReleaseDF, ReleaseConstant.DW_RELEASE_SHOW, saveMode)


      //目标客户 join 曝光
      val csColumns = DWReleaseColumnsHelper.selectDWReleaseCSColumns()
      val csDF = customerReleaseDF.alias("c")
        .join(showReleaseDF.alias("s"), customerReleaseDF("release_session")===showReleaseDF("release_session"), "left")
        .selectExpr(csColumns:_*)
      println(s"csDF================")
      csDF.show(10,false)

      df.unpersist()



    }catch{
      case ex:Exception => {
        println(s"DWReleaseCustomer.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DWReleaseCustomer.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  def handleDropDulJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val df = spark.read.table("test_employee.ods_employee")
      val ndf = df.groupBy($"snum")
        .agg(
          countDistinct("name").alias(s"names"),
          count("ct").alias(s"ct")
        ).orderBy($"names".desc)






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
  def handleJobs(appName :String, bdp_day:String) :Unit = {

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

      //dw
      //handleDWJob(spark, appName, bdp_day)


      //dm
      //handleDMJob(spark, appName, bdp_day)


      //customer+show
      //handleJob(spark, appName, bdp_day)


      handleDropDulJob(spark, appName, bdp_day)


    }catch{
      case ex:Exception => {
        //println(s"DWReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }



  def main(args: Array[String]): Unit = {

    //动态参数接收
    //val Array(appName, bdp_day_begin, bdp_day_end) = args


    val appName: String = "dw_release_customer_job"
    val bdp_day_begin:String = "20190613"
    val bdp_day_end:String = "20190613"
    val bdp_day:String = "20190613"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day)

    val end = System.currentTimeMillis()



  }

}
