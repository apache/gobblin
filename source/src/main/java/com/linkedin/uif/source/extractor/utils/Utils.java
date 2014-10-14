package com.linkedin.uif.source.extractor.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;

public class Utils {

	private static final Gson gson = new Gson();

	private static final String CURRENT_DAY = "CURRENTDAY";
	private static final String CURRENT_HOUR = "CURRENTHOUR";

	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZone(DateTimeZone.forID("America/Los_Angeles"));

	public static String getClause(String clause, String datePredicate) {
		String retStr = "";
		if (!Strings.isNullOrEmpty(datePredicate)) {
			retStr = " " + clause + " (" + datePredicate + ")";
		}
		return retStr;
	}

	public static JsonArray removeElementFromJsonArray(JsonArray inputJsonArray, String key) {
		JsonArray outputJsonArray = new JsonArray();
		for (int i = 0; i < inputJsonArray.size(); i += 1) {
			JsonObject jsonObject = inputJsonArray.get(i).getAsJsonObject();
			outputJsonArray.add(removeElementFromJsonObject(jsonObject, key));
		}
		return outputJsonArray;
	}

	public static JsonObject removeElementFromJsonObject(JsonObject jsonObject, String key) {
		if (jsonObject != null) {
			jsonObject.remove(key);
			return jsonObject;
		}
		return null;
	}

	public static String toDateTimeFormat(String input, String inputfmt, String outputfmt) {
		Date date = null;
		SimpleDateFormat infmt = new SimpleDateFormat(inputfmt);
		try {
			date = infmt.parse(input);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat outFormat = new SimpleDateFormat(outputfmt);
		return outFormat.format(date);
	}

	public static String epochToDate(long epoch, String format) {
		SimpleDateFormat sdf  = new SimpleDateFormat(format);
		Date date = new Date(epoch);
		return sdf.format(date);
	}

	public static long getAsLong(String value) {
		if(Strings.isNullOrEmpty(value)) {
			return 0;
		}
		return Long.parseLong(value);
	}

	public static int getAsInt(String value) {
		if(Strings.isNullOrEmpty(value)) {
			return 0;
		}
		return Integer.parseInt(value);
	}

	public static Date toDate(long value, String format) {
		SimpleDateFormat fmt = new SimpleDateFormat(format);
		Date date = null;
		try {
			date = fmt.parse(Long.toString(value));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	public static Date toDate(Date date, String format) {
		SimpleDateFormat fmt = new SimpleDateFormat(format);
		String dateStr = fmt.format(date);
		Date outDate = null;
		try {
			outDate = fmt.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return outDate;
	}

	public static String dateToString(Date datetime, String format) {
		SimpleDateFormat fmt = new SimpleDateFormat(format);
		return fmt.format(datetime);
	}

	public static Date addHoursToDate(Date datetime, int hours) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(datetime);
		calendar.add(Calendar.HOUR, hours);
		return calendar.getTime();
	}

	public static Date addSecondsToDate(Date datetime, int seconds) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(datetime);
		calendar.add(Calendar.SECOND, seconds);
		return calendar.getTime();
	}

	public static boolean isSimpleWatermark(WatermarkType watermarkType) {
		if(watermarkType == WatermarkType.SIMPLE) {
			return true;
		}
		return false;
	}

	/**
	 * Print time difference in minutes, seconds and milliseconds
	 */
	public static String printTiming(long start, long end) {
		long totalMillis = end - start;
		long mins = TimeUnit.MILLISECONDS.toMinutes(totalMillis);
		long secs = TimeUnit.MILLISECONDS.toSeconds(totalMillis) - TimeUnit.MINUTES.toSeconds(mins);
		long millis = TimeUnit.MILLISECONDS.toMillis(totalMillis) - TimeUnit.MINUTES.toMillis(mins) - TimeUnit.SECONDS.toMillis(secs);
		return String.format("%d min, %d sec, %d millis", mins, secs, millis);
	}

	/**
	 * get column list from the user provided query to build schema with the respective columns
	 * @param input query
     * @return list of columns
	 */
	public static List<String> getColumnListFromQuery(String query) {
		if (Strings.isNullOrEmpty(query)) {
			return null;
		}
		String queryLowerCase = query.toLowerCase();
		int startIndex = queryLowerCase.indexOf("select ") + 7;
		int endIndex = queryLowerCase.indexOf(" from ");
		if(startIndex < 0 || endIndex < 0) {
			return null;
		}
		String[] inputQueryColumns = query.substring(startIndex, endIndex).toLowerCase().replaceAll(" ", "").split(",");
		return Arrays.asList(inputQueryColumns);
	}

	/**
	 * Convert CSV record(List<Strings>) to JsonObject using header(column Names)
	 * @param header record
	 * @param data record
	 * @param column Count
     * @return JsonObject
	 */
	public static JsonObject csvToJsonObject(List<String> bulkRecordHeader, List<String> record, int columnCount) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> resultInfo = new HashMap<String, String>();
        for (int i = 0; i < columnCount; i++) {
            resultInfo.put(bulkRecordHeader.get(i), record.get(i));
        }

        JsonNode json = mapper.valueToTree(resultInfo);
        JsonElement element = gson.fromJson(json.toString(), JsonObject.class);
        return element.getAsJsonObject();
	}

	public static int getAsInt(String value, int defaultValue) {
		return (Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value));
	}

	// escape characters in column name or table name
	public static String escapeSpecialCharacters(String columnName, String escapeChars, String character) {
		if(Strings.isNullOrEmpty(columnName)) {
			return null;
		}

		if(StringUtils.isEmpty(escapeChars)) {
			return columnName;
		}

		List<String> specialChars= Arrays.asList(escapeChars.split(","));
		for (String specialChar: specialChars) {
			columnName = columnName.replace(specialChar, character);
		}
		return columnName;
	}

	/**
	 * Helper method for getting a value containing CURRENTDAY-1 or CURRENTHOUR-1 in the form yyyyMMddHHmmss
	 * @param value
	 * @return
	 */
	public static long getLongWithCurrentDate(String value) {
	  if (Strings.isNullOrEmpty(value)) {
	    return 0;
	  }

	  DateTime time = new DateTime(DateTimeZone.forID("America/Los_Angeles"));
	  if (value.toUpperCase().startsWith(CURRENT_DAY)) {
	    return Long.valueOf(DATE_FORMATTER.print(time.minusDays(Integer.parseInt(value.substring(CURRENT_DAY.length() + 1)))));
	  }
	  if (value.toUpperCase().startsWith(CURRENT_HOUR)) {
      return Long.valueOf(DATE_FORMATTER.print(time.minusHours(Integer.parseInt(value.substring(CURRENT_HOUR.length() + 1)))));
	  }
	  return Long.parseLong(value);
	}

//	public static String JsonArrayToRelational(JsonArray jsonRecords, String colDelimiter, String rowDelimiter) {
//		JsonArray keys = getKeysFromJsonObject(jsonRecords.get(0).getAsJsonObject());
//
//		if (keys == null || keys.size() == 0) {
//			return null;
//		}
//
//		StringBuffer sb = new StringBuffer();
//		for (int i = 0; i < jsonRecords.size(); i += 1) {
//			JsonObject jo = jsonRecords.optJSONObject(i);
//			if (jo != null) {
//				sb.append(MergeJsonValues(jo.toJSONArray(keys), colDelimiter, rowDelimiter));
//			}
//		}
//		return sb.toString();
//	}
//
//	public static String JsonObjectToRelational(JsonObject jsonObject, String colDelimiter) {
//		JsonArray keys = getKeysFromJsonObject(jsonObject);
//
//		if (keys == null || keys.size() == 0) {
//			return null;
//		}
//
//		return MergeJsonValues(jsonObject.toJSONArray(keys), colDelimiter, null);
//	}
//
//	private static JsonArray getKeysFromJsonObject(JsonObject jsonObject) {
//		for (Map.Entry<String,JsonElement> entry : jsonObject.entrySet()) {
//		    String key = entry.getKey();
//		    System.out.println("Key:"+key);
//		}
//
//		if (jsonObject != null) {
//			return jsonObject.entrySet();
//		}
//		return null;
//	}
//
//	public static String MergeJsonValues(JSONArray jsonArray, String colDelimiter, String rowDelimiter) {
//		StringBuffer sb = new StringBuffer();
//		for (int i = 0; i < jsonArray.size(); i += 1) {
//			if (i > 0) {
//				sb.append(colDelimiter);
//			}
//			Object object = jsonArray.opt(i);
//			if (object != null) {
//				String string = object.toString();
//				sb.append(string);
//			}
//		}
//		if(rowDelimiter != null) {
//			sb.append(rowDelimiter);
//		}
//		return sb.toString();
//	}

}
