package com.qf.bigdata.release.etl.udf

import com.qf.bigdata.release.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.codehaus.jackson.map.ObjectMapper

/**
  * Spark UDF
  */
object QFUdf {

  val objectMapper: ObjectMapper = new ObjectMapper()




  /**
    * 时间戳转时间段
    * @param ct
    * @return
    */
  def getTimeSegment(ct :Long):String = {
    var tseg = ""
    if(null != ct){
      try{
        tseg = CommonUtil.getTimeSegment(ct)
      }catch{
        case ex:Exception => {
          println(s"QFUdf.getTimeSegment occur exception：msg=$ex")
        }
      }

    }
    tseg
  }


  /**
    * 用户积分统计
    * @param ct
    * @return
    */
  def getScore(action :String, actionCount:Int):Int = {
    var score = 0;
    if(StringUtils.isNotEmpty(action)){
      try{
        if("01".equals(action)){
          score = actionCount;
        }else if("02".equals(action)){
          score = actionCount * 2;
        }else if("03".equals(action)){
          score = actionCount * 2;
        }else if("04".equals(action)){
          score = actionCount * 3;
        }else if("05".equals(action)){
          score = actionCount * 5;
        }
      }catch{
        case ex:Exception => {
          println(s"QFUdf.getScore occur exception：msg=$ex")
        }
      }
    }
    score
  }




  /**
    * 年龄段
    * @return
    */
  def getAgeRange(age:String): String = {
    var tseg = ""
    if(null != age){
      try{
        tseg = CommonUtil.getAgeRange(age)
      }catch{
        case ex:Exception => {
          println(s"QFUdf.getAgeRange occur exception：msg=$ex")
        }
      }

    }
    tseg
  }

}
