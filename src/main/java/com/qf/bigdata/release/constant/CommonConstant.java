package com.qf.bigdata.release.constant;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;

/**
 * 通用常量
 */
public class CommonConstant implements Serializable {

    //时间格式
    public static final DateTimeFormatter PATTERN_YYYYMMDD =  DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final DateTimeFormatter PATTERN_YYYYMMDD_MID =  DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter PATTERN_HOUR =  DateTimeFormatter.ofPattern("HH");


    //===charset====================================================================
    public static final String CHARSET_UTF8 = "utf-8"; //测试通道


    //===常用符号====================================================================

    public static final String Encoding_UTF8 = "UTF-8";
    public static final String Encoding_GBK = "GBK";

    public static final String MIDDLE_LINE = "-";
    public static final String BOTTOM_LINE = "_";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String PLINE = "|";
    public static final String COLON = ":";
    public static final String PATH_W = "\\";
    public static final String PATH_L = "/";
    public static final String POINT = ".";
    public static final String BLANK = " ";

    public static final String LEFT_ARROWS = "<-";
    public static final String RIGHT_ARROWS = "->";

    public static final String LEFT_BRACKET = "[";
    public static final String RIGHT_BRACKET = "]";

    public static final String TAB = "\t";

}
