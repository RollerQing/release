package com.qf.bigdata.release.util

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.udf.QFUdf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{Dependency, SparkConf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by finup on 2018/5/14.
  */

case class JDBCCase(url :String, user:String, password:String)

object SparkHelper {

  val logger :Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  //缓存级别
  var storageLevels :mutable.Map[String,StorageLevel] = mutable.Map[String,StorageLevel]()




  /**
    * 创建sparksession
    * @param sconf
    * @return
    */
  def createSpark(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }

  def createSparkNotHive(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }



  /**
    * udf注册
    * @param spark
    */
  def registerFun(spark: SparkSession):Unit={
    //时间段
    spark.udf.register("getTimeSegment", QFUdf.getTimeSegment _)

    //年龄段
    spark.udf.register("getAgeRange", QFUdf.getAgeRange _)
  }


  /**
    * 重分区
    * @param df
    * @return
    */
  def repartitionsDF(df:DataFrame) : DataFrame = {
    val partitions = df.rdd.partitions.length
    val childPartitions :Int = partitions./(ReleaseConstant.DEF_PARTITIONS_FACTOR)
    //println(s"parent.partitions=${partitions},childPartitions=${childPartitions}")
    df.repartition(childPartitions)
  }


  /**
    * 依赖关系
    * @param df
    * @return
    */
  def dependenciesRelation(df:DataFrame, dfName:String) : Unit = {
    val dependencies:Seq[Dependency[_]] = df.rdd.dependencies
    println(s"df[${dfName}]=${dependencies}")
  }

  /**
    * 数据转换
    */
  def convertData(values :mutable.Map[String,Object], statusFields: Seq[String]): mutable.Map[String, Object] ={
    values.filter(schemaField => {
      val key = schemaField._1
      statusFields.contains(key)
    })
  }

  /**
    * 基本列选取
    * @return
    */
  def convertColumns(df:DataFrame, cols:Seq[String]):ArrayBuffer[Column] ={
    val columns = new ArrayBuffer[Column]()
    for(col <- cols){
      columns.+=(df(col))
    }
    columns
  }



  /**
    * 时间格式化
    * @param timestamp
    * @param formatter
    * @return
    */
  def formatDate(timestamp:Long, formatter:String="yyyyMMdd"):String={
    var formatDate :Date = new Date(timestamp)
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat(formatter)
    var datestr = dateFormat.format(formatDate)
    datestr
  }






  /**
    * 参数校验
    * @return
    */
  def rangeDate(day:String, range:Int):Seq[String] = {
    val bdp_days =  new ArrayBuffer[String]()
    try{
      val bdp_date_begin = day.toInt
      var cday = day
      var loop = 0

      bdp_days.+=(day)
      while(range > loop){
        val pday = DateUtil.dateFormat4StringDiff(cday, -1)
        bdp_days.+=(pday)
        cday = pday
        loop += 1
      }

    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }

    bdp_days
  }


  /**
    * 参数校验
    * @return
    */
  def rangeDates(begin:String, end:String):Seq[String] = {
    val bdp_days =  new ArrayBuffer[String]()
    try{
      val bdp_date_begin = DateUtil.dateFormat4String(begin,"yyyyMMdd")
      val bdp_date_end = DateUtil.dateFormat4String(end,"yyyyMMdd")

      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while(cday != bdp_date_end){
          bdp_days.+=(cday)
          val pday = DateUtil.dateFormat4StringDiff(cday, 1)
          cday = pday
        }
      }

    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }

    bdp_days
  }




  /**
    * 创建schema
    * @param schema
    * @param delFields
    * @param addFields
    * @return
    */
  def createSchema(schema: StructType, delFields:Array[String], addFields: Seq[StructField]): StructType = {
    val fields = schema.fields
    val newFields = createFields(fields, delFields, addFields)
    new StructType(newFields)
  }


  /**
    * 构造新schema
    * @param schema
    * @return
    */
  def createSchema(schema: StructType, useFields:Seq[String]): StructType = {
    val fields = schema.fields
    val newFieldList :mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    val filterFields = fields.filter((field:StructField) => {
      useFields.contains(field.name)
    })

    newFieldList.++=(filterFields)
    new StructType(newFieldList.toArray)
  }

  /**
    * 创建新字段
    * @param fields
    * @param delFields
    * @param addFields
    * @return
    */
  def createFields(fields:Array[StructField], delFields:Array[String], addFields: Seq[StructField]): Array[StructField] ={
    val newFieldList :mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    val filterFields = fields.filter((field:StructField) => {
      !delFields.contains(field.name)
    })
    newFieldList.++=(filterFields)
    for(addField <- addFields){
      newFieldList.+=(addField)
    }
    newFieldList.toArray
  }

  def parentPath(path :String) :String = {
    var parentPath = path
    if(StringUtils.isNotBlank(path)){
      parentPath = path.substring(0, path.lastIndexOf('/')+1)
    }
    parentPath
  }


  /**
    * jdbc数据源配置
    * @param path
    * @return
    */
  def readJDBC(path:String): JDBCCase ={
    var jdbc :JDBCCase= null
    if(StringUtils.isNotBlank(path)) {
      val pro:Properties = PropertyUtil.readProperties(path)
      val user = pro.getProperty(ReleaseConstant.JDBC_CONFIG_USER)
      val password = pro.getProperty(ReleaseConstant.JDBC_CONFIG_PASS)
      val url = pro.getProperty(ReleaseConstant.JDBC_CONFIG_URL)
      jdbc = JDBCCase(url, user, password)
    }
    jdbc
  }



  /**
    * mysql存储聚合数据
    * @param spark
    * @param df
    */
  def hive2mysql(spark:SparkSession, df:DataFrame, tableName:String, columns:Seq[String]): Unit ={

    //写入mysql
    val mysqlDF = df.selectExpr(columns:_*)

    //mysql
    val jdbcConf : JDBCCase = readJDBC(ReleaseConstant.JDBC_CONFIG_PATH)
    if(null != jdbcConf){
      val url = jdbcConf.url
      val user = jdbcConf.user
      val password = jdbcConf.password

      val prop = new Properties()
      prop.put(ReleaseConstant.JDBC_CONFIG_USER, user)
      prop.put(ReleaseConstant.JDBC_CONFIG_PASS, password)

      mysqlDF.write.mode(SaveMode.Append).jdbc(url,tableName,prop)
    }
  }

  /**
    * 读取数据表
    */
  def readTableData(spark:SparkSession, tableName:String, colNames:mutable.Seq[String]) :DataFrame ={
    val begin = System.currentTimeMillis()
    import spark.implicits._
    println(s"readTableData.table[$tableName], col[${colNames.mkString}]")
    val tableDF = spark.read.table(tableName)
      .selectExpr(colNames:_*)
    tableDF
  }

  def readTableData(spark:SparkSession, tableName:String) :DataFrame ={
    val begin = System.currentTimeMillis()
    import spark.implicits._
    println(s"readTableData.table[$tableName]]")
    val tableDF = spark.read.table(tableName)
    tableDF
  }


  /**
    * 读取数据表
    */
  def readTableData4Condition(spark:SparkSession, tableName:String, colNames:mutable.Seq[String], conditions:Column, storageLevel:StorageLevel) :DataFrame ={
    val begin = System.currentTimeMillis()
    import spark.implicits._
    println(s"readTableData.table[$tableName], col[${colNames.mkString}], conditions===>$conditions")
    val tableDF = spark.read.table(tableName)
      .where(conditions)
      .selectExpr(colNames:_*)
      .persist(storageLevel)
    tableDF
  }

  /**
    * 写入数据表
    */
  def writeTableData(sourceDF :DataFrame, table:String, mode:SaveMode) :Unit ={
    val begin = System.currentTimeMillis()
    //写入表数据
    sourceDF.write.mode(mode).insertInto(table)
    println(s"table[$table] use:${System.currentTimeMillis() - begin}=========================>")
  }



  /**
    * mysql存储聚合数据
    * @param spark
    */
  def writeToMysql(df:DataFrame, tableName:String, saveMode: SaveMode): Unit ={
    //mysql
    val jdbcPro :Properties = PropertyUtil.readProperties(ReleaseConstant.JDBC_CONFIG_PATH)
    if(null != jdbcPro){
      val url = jdbcPro.getProperty(ReleaseConstant.JDBC_CONFIG_URL)
      df.write.mode(saveMode).jdbc(url,tableName,jdbcPro)
    }
  }

  /**
    * DB表中取数
    * @param spark
    * @param db_url
    * @param db_table
    * @param db_user
    * @param db_pass
    * @return
    */
  def getDBData(spark:SparkSession, db_url:String, db_table:String, db_user:String, db_pass:String) :Array[Row] = {
    val prop = new Properties()
    prop.put("user", db_user)
    prop.put("password", db_pass)
    val df = spark.read.jdbc(db_url,db_table,prop)
    df.collect()
  }


//  /**
//    * 数据源处理
//    */
//  def readTableData4SQL(spark:SparkSession, hql:String,colNames:Option[mutable.Seq[String]], storageLevel:StorageLevel) :DataFrame ={
//    val begin = System.currentTimeMillis()
//    import spark.implicits._
//    println(s"readTableData4SQL.hql===>$hql")
//    val sourceDF = spark.sql(hql)
//
//    val tableDF = colNames match {
//      case cols : Seq[String] => sourceDF.selectExpr(cols:_*)
//      case None =>  sourceDF
//    }
//
//    tableDF.persist(storageLevel)
//    tableDF
//  }

  def main(args: Array[String]): Unit = {




  }


}
