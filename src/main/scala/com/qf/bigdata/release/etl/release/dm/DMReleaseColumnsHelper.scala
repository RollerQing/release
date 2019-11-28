package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {

  /**
    * 目标客户
    *
    * @return
    */
  def selectDMCustomerSourcesColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 目标客户Cube
    * @return
    */
  def selectDMCustomerCubeColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 目标客户
    * @return
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光
    * @return
    */
  def selectDWReleaseExposureColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange('age') as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光集市
    *
    * @return
    */
  def selectDMReleaseExposureColumns():ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光cube
    * @return
    */
  def selectDMExposureCubeColumns():ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }


}
