package com.linkedin.uif.source.extractor.extract.restapi;

import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.watermark.Predicate;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.RestApiClientException;
import com.linkedin.uif.source.extractor.exception.RestApiConnectionException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.resultset.RecordSet;
import com.linkedin.uif.source.extractor.resultset.RecordSetList;
import com.linkedin.uif.source.extractor.schema.Schema;
import com.linkedin.uif.source.extractor.utils.Utils;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An implementation of salesforce extractor for to extract data from SFDC
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public class SalesforceExtractor<S, D> extends RestApiExtractor<S, D> {
	private static final String DEFAULT_SERVICES_DATA_PATH = "/services/data";
	private static final String SOQL_RESOURCE = "/query";
	private static final String DEFAULT_AUTH_TOKEN_PATH = "/services/oauth2/token";
	private static final String SALESFORCE_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";
	private static final String SALESFORCE_DATE_FORMAT = "yyyy-MM-dd";
	private static final String SALESFORCE_HOUR_FORMAT = "HH";
	private static final Gson gson = new Gson();
	
	private boolean pullStatus = true;
	private String nextUrl;
	private String servicesDataEnvPath;
	protected Logger log = LoggerFactory.getLogger(SalesforceExtractor.class);
	
	public SalesforceExtractor(WorkUnitState state) {
		super(state);
	}

	public String getServicesDataEnvPath() {
		return servicesDataEnvPath;
	}

	public void setServicesDataEnvPath(String servicesDataEnvPath) {
		this.servicesDataEnvPath = servicesDataEnvPath;
	}
	
	/**
	 * true is further pull required else false
	 */
	public void setPullStatus(boolean pullStatus) {
		this.pullStatus = pullStatus;
	}

	/**
	 * url for the next pull from salesforce
	 */
	public void setNextUrl(String nextUrl) {
		this.nextUrl = nextUrl;
	}
	
	@Override
	public HttpEntity getAuthentication() throws RestApiConnectionException {
		this.log.debug("Authenticating salesforce");
		String clientId = this.workUnit.getProp(ConfigurationKeys.SOURCE_CLIENT_ID);
		String clientSecret = this.workUnit.getProp(ConfigurationKeys.SOURCE_CLIENT_SECRET);
		String userName = this.workUnit.getProp(ConfigurationKeys.SOURCE_USERNAME);
		String password = this.workUnit.getProp(ConfigurationKeys.SOURCE_PASSWORD);
		String securityToken = this.workUnit.getProp(ConfigurationKeys.SOURCE_SECURITY_TOKEN);
		String host = this.workUnit.getProp(ConfigurationKeys.SOURCE_HOST_NAME);

		List<NameValuePair> formParams = new ArrayList<NameValuePair>();
		formParams.add(new BasicNameValuePair("grant_type", "password"));
		formParams.add(new BasicNameValuePair("client_id", clientId));
		formParams.add(new BasicNameValuePair("client_secret", clientSecret));
		formParams.add(new BasicNameValuePair("username", userName));
		formParams.add(new BasicNameValuePair("password", password + securityToken));
		try {
			HttpPost post = new HttpPost(host + DEFAULT_AUTH_TOKEN_PATH);
			post.setEntity(new UrlEncodedFormEntity(formParams));

			HttpResponse httpResponse = getHttpClient().execute(post);
			HttpEntity httpEntity = httpResponse.getEntity();

			return httpEntity;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RestApiConnectionException("Failed to authenticate salesforce using user:" + userName + " and host:" + host + "; error-"
					+ e.getMessage());
		}
	}

	@Override
	public String getSchemaMetadata(String schema, String entity) throws SchemaException {
		this.log.debug("Build url to retrieve schema");
		return  this.getFullUri("/sobjects/" + entity.trim() + "/describe");
	}

	@Override
	public JsonArray getSchema(String response) throws SchemaException {
		this.log.info("Get schema from salesforce:");
		JsonArray fieldJsonArray = new JsonArray();
		JsonElement element = gson.fromJson(response, JsonObject.class);
		JsonObject jsonObject = element.getAsJsonObject();

		try {
			JsonArray array = jsonObject.getAsJsonArray("fields");
			for (JsonElement columnElement : array) {
				JsonObject field = columnElement.getAsJsonObject();
				Schema schema = new Schema();
				schema.setColumnName(field.get("name").getAsString());
				
				String dataType = field.get("type").getAsString();
				String elementDataType = "string";
				List<String> mapSymbols = null;
				JsonObject newDataType = this.convertDataType(field.get("name").getAsString(), dataType, elementDataType, mapSymbols);
				this.log.debug("ColumnName:"+field.get("name").getAsString()+";   old datatype:"+dataType+";   new datatype:"+newDataType);
				
				schema.setDataType(newDataType);
				schema.setLength(field.get("length").getAsLong());
				schema.setPrecision(field.get("precision").getAsInt());
				schema.setScale(field.get("scale").getAsInt());
				schema.setNullable(field.get("nillable").getAsBoolean());
				schema.setFormat(null);
				schema.setComment((field.get("label").isJsonNull() ? null : field.get("label").getAsString()));
				schema.setDefaultValue((field.get("defaultValue").isJsonNull() ? null : field.get("defaultValue").getAsString()));
				schema.setUnique(field.get("unique").getAsBoolean());

				String jsonStr = gson.toJson(schema);
				JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
				fieldJsonArray.add(obj);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new SchemaException("Failed to get schema from salesforce; error-" + e.getMessage());
		}
		return fieldJsonArray;
	}

	@Override
	public String getHighWatermarkMetadata(String schema, String entity, String watermarkColumn, List<Predicate> predicateList)
			throws HighWatermarkException {
		this.log.debug("Build url to retrieve high watermark");
		String query = "SELECT " + watermarkColumn + " FROM " + entity;
		String defaultPredicate = " " + watermarkColumn + " != null";
		String defaultSortOrder = " ORDER BY " + watermarkColumn + " desc LIMIT 1";
		
		String existingPredicate = "";
		if (this.updatedQuery != null) {
			String queryLowerCase = this.updatedQuery.toLowerCase();
			int startIndex = queryLowerCase.indexOf(" where ");
			if (startIndex > 0) {
				existingPredicate = this.updatedQuery.substring(startIndex);
			}
		}
		query = query + existingPredicate;

		Iterator<Predicate> i = predicateList.listIterator();
		while (i.hasNext()) {
			Predicate predicate = i.next();
			query = this.addPredicate(query, predicate.getCondition());
		}
		query = this.addPredicate(query, defaultPredicate);
		query = query + defaultSortOrder;
		this.log.info("QUERY:" + query);

		try {
			return this.getFullUri(this.getSoqlUrl(query));
		} catch (Exception e) {
			e.printStackTrace();
			throw new HighWatermarkException("Failed to get salesforce url for high watermark; error-" + e.getMessage());
		}
	}

	@Override
	public long getHighWatermark(String response, String watermarkColumn, String format) throws HighWatermarkException {
		this.log.info("Get high watermark from salesforce");
		JsonElement element = gson.fromJson(response, JsonObject.class);
		long high_ts;
		try {
			JsonObject jsonObject = element.getAsJsonObject();
			JsonArray jsonArray = jsonObject.getAsJsonArray("records");
			if (jsonArray.size() == 0) {
				return -1;
			}

			String value = jsonObject.getAsJsonArray("records").get(0).getAsJsonObject().get(watermarkColumn).getAsString();
			if (format != null) {
				SimpleDateFormat inFormat = new SimpleDateFormat(format);
				Date date = null;
				try {
					date = inFormat.parse(value);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				SimpleDateFormat outFormat = new SimpleDateFormat("yyyyMMddHHmmss");
				high_ts = Long.parseLong(outFormat.format(date));
			} else {
				high_ts = Long.parseLong(value);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new HighWatermarkException("Failed to get high watermark from salesforce; error-" + e.getMessage());
		}
		return high_ts;
	}

	@Override
	public String getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws RecordCountException {
		this.log.debug("Build url to retrieve source record count");
		String existingPredicate = "";
		if (this.updatedQuery != null) {
			String queryLowerCase = this.updatedQuery.toLowerCase();
			int startIndex = queryLowerCase.indexOf(" where ");
			if (startIndex > 0) {
				existingPredicate = this.updatedQuery.substring(startIndex);
			}
		}

		String query = "SELECT COUNT() FROM " + entity + existingPredicate;
		try {
			if (isNullPredicate(predicateList)) {
				this.log.info("QUERY:" + query);
				return this.getFullUri(this.getSoqlUrl(query));
			} else {
				Iterator<Predicate> i = predicateList.listIterator();
				while (i.hasNext()) {
					Predicate predicate = i.next();
					query = this.addPredicate(query, predicate.getCondition());
				}

				query = query+this.getLimitFromInputQuery(this.updatedQuery);
				this.log.info("QUERY:" + query);
				return this.getFullUri(this.getSoqlUrl(query));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RecordCountException("Failed to get salesforce url for record count; error-" + e.getMessage());
		}
	}

	@Override
	public long getCount(String response) throws RecordCountException {
		this.log.info("Get source record count from salesforce");
		JsonElement element = gson.fromJson(response, JsonObject.class);
		long count;
		try {
			JsonObject jsonObject = element.getAsJsonObject();
			count = jsonObject.get("totalSize").getAsLong();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RecordCountException("Failed to get record count from salesforce; error-" + e.getMessage());
		}

		return count;
	}

	@Override
	public String getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws DataRecordException {
		this.log.debug("Build url to retrieve data records");
		String query = this.updatedQuery;
		String url = null;
		try {
			if (this.getNextUrl() != null && this.pullStatus == true) {
				url = this.getNextUrl();
			} else {
				if (isNullPredicate(predicateList)) {
					this.log.info("QUERY:" + query);
					return this.getFullUri(this.getSoqlUrl(query));
				}

				String limitString = this.getLimitFromInputQuery(query);
				query = query.replace(limitString, "");
				
				Iterator<Predicate> i = predicateList.listIterator();
				while (i.hasNext()) {
					Predicate predicate = i.next();
					query = this.addPredicate(query, predicate.getCondition());
				}

				query = query+limitString;
				this.log.info("QUERY:" + query);
				url = this.getFullUri(this.getSoqlUrl(query));
			}

			return url;

		} catch (Exception e) {
			e.printStackTrace();
			throw new DataRecordException("Failed to get salesforce url for data records; error-" + e.getMessage());
		}
	}

	private String getLimitFromInputQuery(String query) {
		String inputQuery = query.toLowerCase();
		int limitIndex = inputQuery.indexOf(" limit");
		if(limitIndex>0) {
			return query.substring(limitIndex);
		}
		return "";
	}

	@Override
	public RecordSet<D> getData(String response) throws DataRecordException {
		this.log.debug("Get data records from response");
		RecordSetList<D> rs = new RecordSetList<D>();
		JsonElement element = gson.fromJson(response, JsonObject.class);
		JsonArray partRecords;
		try {
			JsonObject jsonObject = element.getAsJsonObject();

			partRecords = jsonObject.getAsJsonArray("records");
			if (jsonObject.get("done").getAsBoolean()) {
				this.setPullStatus(false);
			} else {
				this.setNextUrl(this.getFullUri(jsonObject.get("nextRecordsUrl").getAsString().replaceAll(this.servicesDataEnvPath, "")));
			}

			JsonArray array = Utils.removeElementFromJsonArray(partRecords, "attributes");
			Iterator<JsonElement> li = array.iterator();
			while (li.hasNext()) {
				JsonElement recordElement = li.next();
				rs.add((D) recordElement);
			}
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataRecordException("Failed to get records from salesforce; error-" + e.getMessage());
		}
	}

	@Override
	public boolean getPullStatus() {
		return this.pullStatus;
	}
	
	@Override
	public String getNextUrl() {
		return nextUrl;
	}
	
	public String getSoqlUrl(String soqlQuery) throws RestApiClientException {
		String path = SOQL_RESOURCE + "/";
		NameValuePair pair = new BasicNameValuePair("q", soqlQuery);
		List<NameValuePair> qparams = new ArrayList<NameValuePair>();
		qparams.add(pair);
		return this.buildUrl(path, qparams);
	}
	
	public String buildUrl(String path, List<NameValuePair> qparams) throws RestApiClientException {
		URIBuilder builder = new URIBuilder();
		builder.setPath(path);
		ListIterator<NameValuePair> i = qparams.listIterator();
		while (i.hasNext()) {
			NameValuePair keyValue = i.next();
			builder.setParameter(keyValue.getName(), keyValue.getValue());
		}
		URI uri;
		try {
			uri = builder.build();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RestApiClientException("Failed to build url; error-" + e.getMessage());
		}
		return new HttpGet(uri).getURI().toString();
	}
	
	protected String addPredicate(String query, String predicateCond) {
		String predicate = "where";
		if (query.toLowerCase().contains(predicate)) {
			predicate = "and";
		}
		query = query + Utils.getClause(predicate, predicateCond);
		return query;
	}
	
	private String getServiceBaseUrl() {
		String dataEnvPath = DEFAULT_SERVICES_DATA_PATH + "/" + this.workUnit.getProp(ConfigurationKeys.SOURCE_VERSION);
		this.setServicesDataEnvPath(dataEnvPath);
		return this.instanceUrl + dataEnvPath;
	}
	
	public String getFullUri(String resourcePath) {
		return StringUtils.removeEnd(getServiceBaseUrl(), "/") + StringUtils.removeEnd(resourcePath, "/");
	}
	
	public boolean isNullPredicate(List<Predicate> predicateList) {
		if(predicateList == null || predicateList.size() == 0) {
			return true;
		}
		return false;
	}
	
	@Override
	public String getWatermarkSourceFormat(WatermarkType watermarkType) {
		switch(watermarkType) {
		case TIMESTAMP:
			return "yyyy-MM-dd'T'HH:mm:ss";
		case DATE:
			return "yyyy-MM-dd";
		default:
			return null;
		}
	}
	
	@Override
	public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
		this.log.info("Getting hour predicate from salesforce");
		String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value),valueFormat,SALESFORCE_HOUR_FORMAT);
		return  column + " " + operator + " " +  Formattedvalue;
	}
	
	@Override
	public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
		this.log.info("Getting date predicate from salesforce");
		String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value),valueFormat,SALESFORCE_DATE_FORMAT);
		return  column + " " + operator + " " +  Formattedvalue;
	}
	
	@Override
	public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
		this.log.info("Getting timestamp predicate from salesforce");
		String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value),valueFormat,SALESFORCE_TIMESTAMP_FORMAT);
		return  column + " " + operator + " " +  Formattedvalue;
	}
	
	@Override
	public Map<String, String> getDataTypeMap() {
		Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder()
				.put("url", "string")
				.put("textarea", "string")
				.put("reference", "string")
				.put("phone", "string")
				.put("masterrecord", "string")
				.put("location", "string")
				.put("id", "string")
				.put("encryptedstring", "string")
				.put("email", "string")
				.put("DataCategoryGroupReference", "string")
				.put("calculated", "string")
				.put("anyType", "string")
				.put("address", "string")
				.put("blob", "string")
				.put("date", "date")
				.put("datetime", "timestamp")
				.put("time", "time")
				.put("object", "string")
				.put("string", "string")
				.put("int", "int")
				.put("long", "long")
				.put("double", "double")
				.put("percent", "double")
				.put("currency", "double")
				.put("decimal", "double")
				.put("boolean", "boolean")
				.put("picklist", "string")
				.put("multipicklist", "string")
				.put("combobox", "string")
				.put("list", "string")
				.put("set", "string")
				.put("map", "string")
				.put("enum", "string").build();
		return dataTypeMap;
	}
}
