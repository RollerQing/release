package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.enums.BiddingResultEnum

import scala.collection.mutable.ArrayBuffer

/**
  * DW层投放业务列表
  */
object DWReleaseColumnsHelper {
  /**
    * 曝光
    * 列和目标客户一样，可以在该方法中直接调用selectDWReleaseCustomerColumns()。
    * 但是鉴于selectDWReleaseCustomerColumns()方法有目的性，所以未直接调用。
    * 可以考虑抽出一个具有公共列的方法，供其他方法调用即可，减少代码量
    * @return
    */
  def selectDWReleaseExposureColumns():ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) ) as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 目标客户
    *
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
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) ) as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 竞价
    *
    * @return
    */
  def selectODSReleaseBiddingColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.bidding_type') as bidding_type")
    columns.+=("get_json_object(exts,'$.bidding_price') as bidding_price")
    columns.+=("get_json_object(exts,'$.bidding_status') as bidding_status")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 竞价
    *
    * @return
    */
  def selectDWReleaseBiddingColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("b.release_session")
    columns.+=("b.release_status")
    columns.+=("b.device_num")
    columns.+=("b.device_type")
    columns.+=("b.sources")
    columns.+=("b.channels")
    columns.+=("b.bidding_type")
    columns.+=("b.bidding_price")
    columns.+=("if(isnotnull(w.bidding_price),w.bidding_price,0) as cost_price")
    columns.+=(s"if(isnotnull(w.bidding_price),${BiddingResultEnum.SUC.getCode},${BiddingResultEnum.FAIL.getCode}) as bidding_result")
    columns.+=("b.aid")
    columns.+=("b.ct")
    columns.+=("b.bdp_day")
    columns
  }


  /**
    * 竞价
    *
    * @return
    */
  def selectDWReleasePMPBiddingColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("bidding_type")
    columns.+=("bidding_price")
    columns.+=("bidding_price as cost_price")
    columns.+=(s"${BiddingResultEnum.SUC.getCode} as bidding_result")
    columns.+=("aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  def selectDWReleaseShowColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  def selectDWReleaseCSColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("c.release_session")
    columns.+=("c.release_status as c_status")
    columns.+=("s.device_num")
    columns.+=("s.device_type")
    columns.+=("c.sources")
    columns.+=("c.channels")
    columns.+=("c.idcard")
    columns.+=("c.ct as c_ct")
    columns.+=("s.ct as s_ct")
    columns.+=("c.bdp_day")
    columns
  }

}
