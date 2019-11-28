package com.qf.bigdata.release.util;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qf.bigdata.release.constant.CommonConstant;
import com.qf.bigdata.release.enums.AgeRangerEnum;
import com.qf.bigdata.release.enums.DaySegmentEnum;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonUtil {

	private static Logger log = LoggerFactory.getLogger(CommonUtil.class);

	private CommonUtil() {
	}



	public static String ConvertStream2Json(InputStream inputStream)
	{
		String jsonStr = "";
		if(null != inputStream ){
			// ByteArrayOutputStream相当于内存输出流
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int len = 0;
			// 将输入流转移到内存输出流中
			try
			{
				while ((len = inputStream.read(buffer, 0, buffer.length)) != -1)
				{
					out.write(buffer, 0, len);
				}
				// 将内存流转换为字符串
				jsonStr = new String(out.toByteArray());
			}catch (IOException e){
				e.printStackTrace();
			}
		}

		return jsonStr;
	}


	//---判空操作--------------------------------------------

	/**
	 * 数组对象是否为空
	 * @param content
	 * @return
     */
	public static boolean isEmpty4Array(Object[] content) {
		boolean flag = true;
		if (null != content && content.length>0) {
			flag = false;
		}
		return flag;
	}

	/**
	 * 集合是否为空
	 * 
	 * @param coll
	 * @return
	 */
	public static boolean isEmpty4Collection(Collection coll) {
		boolean result = true;
		if (null != coll && !coll.isEmpty()) {
			result = false;
		}
		return result;
	}

	public static boolean isEmpty4Map(Map map) {
		boolean result = true;
		if (null != map && !map.isEmpty()) {
			result = false;
		}
		return result;
	}

	//---集合相关---------------------------------
	
	public static int indexOfList(List<Comparable> objList, Comparable obj) {
		int result = -1;
		if (!isEmpty4Collection(objList) && null != obj) {
			
		}
		return result;
	}
	
	
	public static boolean volContain(Collection<? extends Object> coll,Object obj) {
		boolean result = false;
		if (null != coll && !coll.isEmpty() && null != obj) {
			for(Object o : coll){
				if(o.getClass().getName().equals(obj.getClass().getName())){
					//System.out.println("" + o.toString() + "||" + obj.toString());
					result = o.equals(obj);
					if(result){
						break;
					}
				}
			}
		}
		return result;
	}

	public static boolean compCollect(Collection data1, Collection data2) {
		boolean result = false;
		if (!CommonUtil.isEmpty4Collection(data1)
				&& !CommonUtil.isEmpty4Collection(data2)) {
			int len1 = data1.size();
			int len2 = data2.size();
			if (len1 == len2) {
				result = data1.containsAll(data2);
			}
		}
		return result;
	}

	public static String getTimeSegment(Long ct) {
		String tseg = "";
		try {
			int tHour = Integer.valueOf(CommonUtil.dateFormatTimestamp2Pattern(ct, CommonConstant.PATTERN_HOUR));

			if(DaySegmentEnum.MORNING_EARLY.getBegin() <= tHour && tHour <= DaySegmentEnum.MORNING_EARLY.getEnd()){
				//凌晨
				tseg = DaySegmentEnum.MORNING_EARLY.getCode();

			}else if(DaySegmentEnum.MORNING.getBegin() <= tHour && tHour <= DaySegmentEnum.MORNING.getEnd()){
				//早晨
				tseg = DaySegmentEnum.MORNING.getCode();

			}else if(DaySegmentEnum.MIDDAY.getBegin() <= tHour && tHour <= DaySegmentEnum.MIDDAY.getEnd()){
				//中午
				tseg = DaySegmentEnum.MIDDAY.getCode();

			}else if(DaySegmentEnum.AFTERNOON.getBegin() <= tHour && tHour <= DaySegmentEnum.AFTERNOON.getEnd()){
				//下午
				tseg = DaySegmentEnum.AFTERNOON.getCode();
			}else if(DaySegmentEnum.NIGHT.getBegin() <= tHour && tHour <= DaySegmentEnum.NIGHT.getEnd()){
				//晚上
				tseg = DaySegmentEnum.NIGHT.getCode();
			}
		} catch (Exception e) {
			log.error("TimeSegmentUdf.error:" + e.getMessage());
		}
		return tseg;
	}


	/**
	 * 年龄段
	 * @param age
	 * @return
	 */
	public static String getAgeRange(String age) {
		String ageRange = "";
		try {
			int ageInt = Integer.valueOf(age);

			if(AgeRangerEnum.AGE_18.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_18.getEnd()){
				ageRange = AgeRangerEnum.AGE_18.getCode();

			}else if(AgeRangerEnum.AGE_18_25.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_18_25.getEnd()){
				ageRange = AgeRangerEnum.AGE_18_25.getCode();

			}else if(AgeRangerEnum.AGE_26_35.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_26_35.getEnd()){
				ageRange = AgeRangerEnum.AGE_26_35.getCode();

			}else if(AgeRangerEnum.AGE_36_45.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_36_45.getEnd()){
				ageRange = AgeRangerEnum.AGE_36_45.getCode();

			}else if(AgeRangerEnum.AGE_45.getBegin() <= ageInt && ageInt <= AgeRangerEnum.AGE_45.getEnd()){
				ageRange = AgeRangerEnum.AGE_45.getCode();

			}
		} catch (Exception e) {
			log.error("getAgeRange.error:" + e.getMessage());
		}
		return ageRange;
	}

	//---字符操作------------------------------------------------------
	public static String fillStr(int num,int total) {
		int len = String.valueOf(num).length();
		int yu = total-len;
		//ystem.out.println(len+"="+yu);
		String format = "%0"+total+"d";
		String str = String.format(format, num);
		return str;
	}

	public static String fillStr(String str,int total, String fill) {
		String result = str;
		if(!StringUtils.isEmpty(str) && !StringUtils.isEmpty(fill)){
			int len = str.length();
			if(len < total){
				int yu = total - len;
				String repeats = StringUtils.repeat(fill, yu);
				result = str + repeats;
			}
		}
		return result;
	}

	/**
	 * 过滤空格
	 * 
	 * @param content
	 * @return
	 */
	public static String filterWhite(String content) {
		String result = "";
		if (!StringUtils.isEmpty(content)) {
			result = trim(content);
		}
		return result;
	}

	public static String trim(String content) {
		if (null != content) {
			String temp = content;
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(temp);
			content = m.replaceAll("");
		}
		return content;
	}

	/**
	 * 是否为数字
	 * 
	 * @param content
	 * @return
	 */
	public static boolean isNumber(String content) {
		boolean flag = false;
		if (!StringUtils.isEmpty(content)) {
			String reg = "\\d+";
			Pattern p = Pattern.compile(reg);
			Matcher match = p.matcher(content);
			if (match.matches()) {
				flag = true;
			}
		}
		return flag;
	}

	//---集合中是否包含元素-----------------------------------------------------------------
	/**
	 * 判断是否在某范围里存在值
	 * 
	 * @param content
	 * @param bounds
	 * @return
	 */
	public static boolean isExist(String content, String[] bounds) {
		boolean flag = false;
		if (!StringUtils.isEmpty(content) && null != bounds && bounds.length > 0) {
			for (int i = 0; i < bounds.length; i++) {
				String temp = bounds[i];
				if (content.equals(temp)) {
					flag = true;
					break;
				}
			}
		}
		return flag;
	}
	
	public static boolean isExist(String dist,String target) {
		boolean flag = false;
		if (!StringUtils.isEmpty(dist) && !StringUtils.isEmpty(target)) {
			int idx = dist.indexOf(target);
			if(idx != -1){
				flag = true;
			}
		}
		return flag;
	}

	/**
	 * 判断是否在某范围里存在值
	 * 
	 * @param content
	 * @return
	 */
	public static boolean isExist(String content, Set datas) {
		boolean flag = false;
		if (!StringUtils.isEmpty(content) && !isEmpty4Collection(datas)) {
			for (Iterator ite = datas.iterator(); ite.hasNext();) {
				String data = (String) ite.next();
				if (data.equals(content)) {
					flag = true;
					break;
				}
			}
		}
		return flag;
	}

	//---日期相关----------------------------------------------

	/**
	 * 日期格式化
	 * @return
	 */
	public static String dateFormatTimestamp2Pattern(Long ct, DateTimeFormatter pattern) {
		if (ct <= 0 || null == pattern) {
			return "";
		}
		LocalDateTime datetime = LocalDateTime.ofInstant(new Date(ct).toInstant(), ZoneId.systemDefault());
		return datetime.format(pattern);
	}

	/**
	 * 日期格式化
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate(Date date, String type) {
		SimpleDateFormat sdf = new SimpleDateFormat(type);
		String result = null;
		try {
			if (null != date) {
				result = sdf.format(date);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static String formatDate4Def(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String result = null;
		try {
			if (null != date) {
				result = sdf.format(date);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 文本转时间
	 * 
	 * @param content
	 * @return
	 */
	public static Date parseText(String content, String dateType) {
		Date date = null;
		if (!StringUtils.isEmpty(content)) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(dateType);
				date = sdf.parse(content);
			} catch (ParseException pe) {
				pe.printStackTrace();
			}
		}
		return date;
	}
	
	public static Date parseText4Def(String content) {
		Date date = null;
		if (!StringUtils.isEmpty(content)) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				date = sdf.parse(content);
			} catch (ParseException pe) {
				pe.printStackTrace();
			}
		}
		return date;
	}

	/**
	 * 时间比较
	 *
	 * @param t1
	 *            时间参数1
	 * @param t2
	 *            时间参数2
	 * @param diff
	 *            时间差
	 * @param type
	 * @return
	 */
	public static boolean overTime(Date t1, Date t2, int diff, int type) {
		boolean result = false;
		if (null != t1 && null != t2) {
			Calendar c1 = Calendar.getInstance();
			c1.setTime(t1);

			Calendar c2 = Calendar.getInstance();
			c2.setTime(t2);

			if (Calendar.YEAR == type) {// 年比较
				int year1 = c1.get(Calendar.YEAR);
				int year2 = c2.get(Calendar.YEAR);
				int diff4Year = year1 - year2;
				if (diff4Year >= diff) {
					result = true;
				}
			} else if (Calendar.MONTH == type) {// 月比较
				int month1 = c1.get(Calendar.MONTH);
				int month2 = c2.get(Calendar.MONTH);
				int diff4Month = month1 - month2;
				if (diff4Month >= diff) {
					result = true;
				}
			} else if (Calendar.DAY_OF_MONTH == type) {// 日比较
				int day1 = c1.get(Calendar.DAY_OF_MONTH);
				int day2 = c2.get(Calendar.DAY_OF_MONTH);
				int diff4Day = day1 - day2;
				if (diff4Day >= diff) {
					result = true;
				}
			} else if (Calendar.HOUR_OF_DAY == type) {// 时比较
				int hour1 = c1.get(Calendar.HOUR_OF_DAY);
				int hour2 = c2.get(Calendar.HOUR_OF_DAY);
				int diff4Hour = hour1 - hour2;
				if (diff4Hour > diff) {
					result = true;
				}
			} else if (Calendar.MINUTE == type) {// 分比较
				int min1 = c1.get(Calendar.MINUTE);
				int min2 = c2.get(Calendar.MINUTE);
				int diff4Min = min1 - min2;
				if (diff4Min > diff) {
					result = true;
				}
			}
		}
		return result;
	}

	public static int compTime(Date one,Date two){
		int result = 1;
		if(null != one && null != two){
			long oneTime = one.getTime();
			long twoTime = two.getTime();
			if(oneTime < twoTime){
				result = -1;
			}else if(oneTime == twoTime){
				result = 0;
			}else{
				result = 1;
			}
		}
		return result;
	}

	public static Date getTime(Date d, int diff, int type){
		Date result = d;
		if(null != d){
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			cal.add(type, diff);
			result = cal.getTime();
		}
		return result;
	}




	//---字符集转换-----------------------------------------------
	/**
	 * 转换编码
	 * 
	 * @param content
	 * @return
	 */
	
	public static String changeChar(String content, String encoding1,
			String encoding2) {
		String temp = null;
		try {
			if (!StringUtils.isEmpty(content)) {
				temp = new String(content.getBytes(encoding1), encoding2);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return temp;
	}

	//---正则-----------------------------------------------------
	/**
	 * 是否为有效身份证
	 * 
	 * @param card
	 * @return
	 */
	public static int getAge4Card(String card) {
		int age = 0;
		int start4Y = 6;
		int start4M = 0;
		int start4D = 0;
		int length4Y = 2;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		if (!StringUtils.isEmpty(card)) {
			int length = card.length();
			if (length == 15) {
				start4M = start4Y + length4Y;
				start4D = start4M + 2;
			} else if (length == 18) {
				length4Y = 4;
				start4M = start4Y + length4Y;
				start4D = start4M + 2;
			}

			int year = Integer.parseInt(card.substring(start4Y, start4M));
			if (length == 15) {
				year = 1900 + year;
			}
			int month = Integer.parseInt(card.substring(start4M, start4D));
			int day = Integer.parseInt(card.substring(start4D, start4D + 2));

			Date curDate = new Date();
			String curStr = sdf.format(curDate);
			int year4Now = Integer.valueOf(curStr.substring(0, 4));
			int month4Now = Integer.valueOf(curStr.substring(5, 7));
			int day4Now = Integer.valueOf(curStr.substring(8, 10));

			int ycha = year4Now - year;
			int mcha = month4Now - month;
			int dcha = day4Now - day;
			if (ycha >= 0) {
				if (mcha == 0) {
					if (dcha < 0) {
						ycha = ycha - 1;
					}
				} else {
					ycha = ycha - 1;
				}
			}

			age = ycha > 0 ? ycha : 0;

		}
		return age;
	}


	public static List ipReg(String ip) {
		List result = new ArrayList();
		if (null != ip) {
			String reg = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
			Pattern pattern = Pattern.compile(reg);
			Matcher matcher = pattern.matcher(ip);
			int groupCount = matcher.groupCount();
			System.out.println("groupCount=" + groupCount);
			for (int i = 1; i < groupCount; i++) {
				System.out.println(matcher.group(i));
			}
		}
		return result;
	}

	//---加密--------------------------------------------------------

	/**
	 * md5
	 * @param source
	 * @return
	 */
	public static String getMD5(byte[] source) {
		String s = null;
		char hexDigits[] = {
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd','e', 'f' };
		try {
			MessageDigest md = MessageDigest
					.getInstance("MD5");
			md.update(source);
			byte tmp[] = md.digest();
			char str[] = new char[16 * 2];
			int k = 0;
			for (int i = 0; i < 16; i++) {
				byte byte0 = tmp[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			s = new String(str);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return s;
	}

	public static String encodePassword(byte[] content, String algorithm,
			String encoding) {
		MessageDigest md = null;
		byte[] unencodedPassword = content;
		String result = null;
		try {
			StringBuffer buf = new StringBuffer();
			md = MessageDigest.getInstance(algorithm);

			md.reset();
			md.update(unencodedPassword);

			byte[] encodedPassword = md.digest();
			for (int i = 0; i < encodedPassword.length; i++) {
				if ((encodedPassword[i] & 0xff) < 0x10) {
					buf.append("0");
				}
				buf.append(Long.toString(encodedPassword[i] & 0xff, 16));
			}

			result = buf.toString();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public static String encodePassword(String password, String algorithm,
			String encoding) {
		MessageDigest md = null;
		byte[] unencodedPassword = null;
		String result = password;
		try {
			StringBuffer buf = new StringBuffer();
			unencodedPassword = password.getBytes(encoding);
			md = MessageDigest.getInstance(algorithm);

			md.reset();
			md.update(unencodedPassword);

			byte[] encodedPassword = md.digest();
			for (int i = 0; i < encodedPassword.length; i++) {
				if ((encodedPassword[i] & 0xff) < 0x10) {
					buf.append("0");
				}
				buf.append(Long.toString(encodedPassword[i] & 0xff, 16));
			}

			result = buf.toString();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public static String uncodePassword(String md5Pass, String algorithm) {
		return new String(Hex.encodeHex(digest(md5Pass.getBytes(), algorithm)))
				.toUpperCase();
	}

	public static byte[] digest(byte[] pd, String algorithm) {
		try {
			MessageDigest md = MessageDigest.getInstance(algorithm);
			md.update(pd);
			return md.digest();
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException("Digest the source failed.cause: "
					+ nsae.getMessage(), nsae);
		}
	}

	// -----------------------------------------------------------------------------


	
	/**
	 * url转码
	 * 
	 * @param content
	 * @return
	 */
	public static String change2UTF84Url(String content) {
		String temp = null;
		try {
			if (!StringUtils.isEmpty(content)) {
				temp = java.net.URLEncoder.encode(content, "UTF-8");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return temp;
	}


	/**
	 * excel标头
	 * 
	 * @param excelHeadStr
	 * @return
	 */
	public static List splitExcelHead(String excelHeadStr) {
		List result = new ArrayList();
		if (!StringUtils.isEmpty(excelHeadStr)) {
			String[] labels = excelHeadStr.split(",");
			for (int i = 0; i < labels.length; i++) {
				String label = labels[i];
				result.add(label);
			}
		}
		return result;
	}


	// ----字符数字转换--------------------------------------------------------------------------------------

	/**
	 * 字节转float
	 * @return
	 */
	public static float getFloat(byte[] b) {
		int accum = 0;
		for (int shiftBy = 0; shiftBy < 4; shiftBy++) {
			accum |= (b[shiftBy] & 0xff) << shiftBy * 8;
		}
		return Float.intBitsToFloat(accum);
	}


	public static String formatNumer(float value) {
		DecimalFormat df = new DecimalFormat("0.0"); // 保留一位小数
		String valueStr = df.format(value);
		return valueStr;
	}

	public static String formatNumer(float value, String format) {
		DecimalFormat df = new DecimalFormat(format); // 保留一位小数
		String valueStr = df.format(value);
		return valueStr;
	}
	
	public static String formatNumerd(Double value, String format) {
		DecimalFormat df = new DecimalFormat(format); // 保留一位小数
		String valueStr = df.format(value);
		return valueStr;
	}




	/**
	 * ip地址测试
	 * 
	 * @param ip
	 */
	public static boolean connURL(String ip, int timeout) {
		boolean result = false;
		try {
			URL url = new URL(ip);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(timeout * 1000);
			int respCode = conn.getResponseCode();
			//System.out.println(respCode);
			if (HttpURLConnection.HTTP_OK == respCode) {
				result = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}



	/**
	 * bytes转换成十六进制字符串
	 */
	public static String byte2HexStr(byte[] b) {
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1)
				hs = hs + "0" + stmp;
			else
				hs = hs + stmp;
			// if (n<b.length-1) hs=hs+":";
		}
		return hs.toUpperCase();
	}
	
	public static String byte2HexStr(Integer num) {
		String hs = "";
		if(null != num){
			hs = Integer.toHexString(num);
		}
		return hs.toUpperCase();
	}
	
	public static byte byte2Hex(Integer num) {
		Byte b = null;
		if(null != num){
			b = num.byteValue();
		}
		return b;
	}
	
	public static String byte2HexStr(String num) {
		String hs = "";
		if(!StringUtils.isEmpty(num)){
			Integer n = Integer.valueOf(num);
			hs = Integer.toHexString(n);
		}
		return hs.toUpperCase();
	}
	
	

	
	/**
	 * 参数2是否存在于参数1的串中
	 * @param context
	 * @param section
	 * @param Ignore 是否忽略大小写
	 * @return
	 */
	public static boolean isInEgore(String context,String section,boolean Ignore){
		boolean result = false;
		if(!StringUtils.isEmpty(context) && !StringUtils.isEmpty(section)){
			if(Ignore){//忽略大小写
				String tempContext = context.toLowerCase();
				String tempSection = section.toLowerCase();
				if(tempContext.indexOf(tempSection) != -1){
					result = true;
				}
			}else {
				if(context.indexOf(section) != -1){
					result = true;
				}
			}
		}
		return result;
	}

	/**
	 *
	 * @param d
	 * @param format #.00
     * @return
     */
	public static String formatDouble(Double d,String format){
		String result = null;
		if(null != d){
			DecimalFormat df1 = new DecimalFormat(format);
			df1.setGroupingUsed(false);
			result = df1.format(d);
		}
		return result;
	}

	public static String splitList2String(List list,String sep){
		String result = null;
		if(!isEmpty4Collection(list) && !StringUtils.isEmpty(sep)){
			StringBuffer sb = new StringBuffer();
			for(int i=0; i<list.size(); i++){
				String s = list.get(i).toString();
				if(i == list.size()-1){
					sb.append(s);
				}else {
					sb.append(s).append(sep);
				}
			}
			
			result = sb.toString();
		}
		return result;
	}

	public static List<String> splitString2List(String info,String sep){
		List<String> result = null;
		if(!StringUtils.isEmpty(info) && !StringUtils.isEmpty(sep)){
			
			String[] infos = info.split(sep);
			for(String inf : infos){
				result.add(inf);
			}
			
		}
		return result;
	}

	
	public static String InputStreamTOString(InputStream in,int size,String encoder) throws Exception{ 
		String result = null;
        ByteArrayOutputStream outStream = null;
        try {
			if (null != in && size > 0 && !StringUtils.isEmpty(encoder)) {
				outStream = new ByteArrayOutputStream();
				byte[] data = new byte[size];
				int count = -1;
				while ((count = in.read(data, 0, size)) != -1) {
					outStream.write(data, 0, count);
				}
				data = null;

				result = new String(outStream.toByteArray(), encoder);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(null != outStream){
				outStream.close();
			}
		}
		return result;
    }  

	public static boolean curSystem(){
		Properties prop = System.getProperties();
		String os = prop.getProperty("os.name");
		System.out.println(os);
		if(os.startsWith("win") || os.startsWith("Win")){
			return false;
		}else{
			return true;
		}
	}

	public static String file2String(String fileName){
		Validate.notEmpty(fileName,"fileName is not empty");

		String content = "";
		try{
			content = IOUtils.toString(CommonUtil.class.getClassLoader().getResourceAsStream(fileName), Charset.forName("utf-8"));

		}catch (Exception e){
			e.printStackTrace();
		}

		return content;
	}

	/**
	 * 属性文件转存map
	 * @param proPath
	 * @return
	 */
	public static Map<String,Object> convertProperties2Map(String proPath){
		Map<String,Object> result = null;
		try{
			Properties properties = PropertyUtil.readProperties(proPath);
			if(null != properties){
				//properties.list(System.out);
				result = new TreeMap<String, Object>((Map)properties);
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return result;
	}

	/**
	 * url末尾检查
	 * @param baseUrl
	 * @return
	 */
	public static String endHandleUrl(String baseUrl){
		String nBaseUrl = baseUrl;
		if(StringUtils.isNotEmpty(baseUrl)){
			if(!baseUrl.endsWith(CommonConstant.PATH_L)){
				nBaseUrl += CommonConstant.PATH_L;
			}
		}
		return nBaseUrl;
	}

	/**
	 * url拼接
	 * @param baseUrl
	 * @param addition
	 * @return
	 */
	public static String handleUrl(String baseUrl,String addition){
		String url = baseUrl;
		if(StringUtils.isNotEmpty(baseUrl) && StringUtils.isNotEmpty(addition)){
			url = endHandleUrl(baseUrl) + addition;
		}
		return url;
	}

	/**
	 * url拼接
	 * @param baseUrl
	 * @param additions
	 * @return
	 */
	public static String handleUrls(String baseUrl,String... additions){
		String url = baseUrl;
		if(StringUtils.isNotEmpty(baseUrl)){
			StringBuffer additionBuffer = new StringBuffer();
			String nBaseUrl = endHandleUrl(baseUrl);
			for(String addition:additions){
				additionBuffer.append(endHandleUrl(addition));
			}
			String adds = additionBuffer.toString();
			url = nBaseUrl + adds;
			int protocolIdx = url.indexOf("://",0)+3;
			String protocolUrl = url.substring(0,protocolIdx);
			String subUrl = url.substring(protocolIdx,url.length()).replaceAll("//","/");
			url = protocolUrl+subUrl;
		}
		return url;
	}

	public static void main(String[] args) throws Exception{





	}

}
