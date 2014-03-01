package com.linkedin.uif.source.extractor.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Utils {

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
		int startIndex = queryLowerCase.indexOf("select") + 6;
		int endIndex = queryLowerCase.indexOf("from ") - 1;
		String[] inputQueryColumns = query.substring(startIndex, endIndex).toLowerCase().replaceAll(" ", "").split(",");
		return Arrays.asList(inputQueryColumns);
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
