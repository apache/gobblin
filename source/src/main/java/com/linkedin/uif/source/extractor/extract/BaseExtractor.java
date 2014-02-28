package com.linkedin.uif.source.extractor.extract;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.config.Predicate;
import com.linkedin.uif.source.extractor.config.WatermarkType;
import com.linkedin.uif.source.extractor.exception.DataRecordException;
import com.linkedin.uif.source.extractor.exception.ExtractPrepareException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.schema.ArrayDataType;
import com.linkedin.uif.source.extractor.schema.DataType;
import com.linkedin.uif.source.extractor.schema.EnumDataType;
import com.linkedin.uif.source.extractor.schema.MapDataType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An implementation of Common extractor for all types of sources
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public abstract class BaseExtractor<D, S> implements ProtocolSpecificLayer<D, S> {
	private static final Log LOG = LogFactory.getLog(BaseExtractor.class);
	private static final long DEFAULT_WATERMARK_VALUE = -1;
	private static final String DEFAULT_WATERMARK_VALUE_FORMAT = "yyyyMMddHHmmss";
	private static final SimpleDateFormat DEFAULT_WATERMARK_TIMESTAMP_FORMAT = new SimpleDateFormat(DEFAULT_WATERMARK_VALUE_FORMAT);
	private static final Gson gson = new Gson();
	protected WorkUnitState workUnitState;
	protected WorkUnit workUnit;
	private String entity;
	private String schema;
	private String predicateColumnFormat;
	private boolean fetchStatus = true;
	private S outputSchema;
	private long sourceRecordCount = 0;
	private long highWatermark;

	private Iterator<D> iterator;
	protected List<String> columnList = new ArrayList<String>();
	private List<Predicate> predicateList = new ArrayList<Predicate>();

	private S getOutputSchema() {
		return this.outputSchema;
	}

	protected void setOutputSchema(S outputSchema) {
		this.outputSchema = outputSchema;
	}

	private long getSourceRecordCount() {
		return sourceRecordCount;
	}

	public boolean getFetchStatus() {
		return fetchStatus;
	}

	public void setFetchStatus(boolean fetchStatus) {
		this.fetchStatus = fetchStatus;
	}

	public void setHighWatermark(long highWatermark) {
		this.highWatermark = highWatermark;
	}

	private boolean isPullRequired() {
		return getFetchStatus();
	}

	private boolean isInitialPull() {
		return iterator == null;
	}

	public BaseExtractor(WorkUnitState workUnitState) throws ExtractPrepareException {
		this.workUnitState = workUnitState;
		this.workUnit = this.workUnitState.getWorkunit();
		this.schema = this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA);
		this.entity = this.workUnit.getProp(ConfigurationKeys.SOURCE_ENTITY);
		try {
			long startTs = System.currentTimeMillis();
			LOG.info("Start - preparing the extract");
			this.build();
			long endTs = System.currentTimeMillis();
			LOG.info("End - extract preperation: Time taken: " + this.printTiming(startTs, endTs));
		} catch (ExtractPrepareException e) {
			throw new ExtractPrepareException("Failed to prepare extract; error-" + e.getMessage());
		}
	}

	/**
	 * Read a data record from source
	 * 
	 * @throws DataRecordException,IOException if it can't read data record
	 * @return record of type D
	 */
	public D readRecord() throws DataRecordException, IOException {
		if (!this.isPullRequired()) {
			LOG.debug("No more records");
			return null;
		}

		D nextElement = null;
		try {
			if (isInitialPull()) {
				LOG.debug("initial pull");
				iterator = this.getRecordSet(this.schema, this.entity, this.workUnit, this.predicateList);
			}

			if (iterator.hasNext()) {
				nextElement = iterator.next();
				
				if(iterator.hasNext()) {
				} else {
					LOG.debug("next pull");
					iterator = this.getRecordSet(this.schema, this.entity, this.workUnit, this.predicateList);
					if (iterator == null) {
						this.setFetchStatus(false);
					}
				}
			}
		} catch (Exception e) {
			throw new DataRecordException("Failed to get records using rest api; error-" + e.getMessage());
		}
		return nextElement;
	};

	/**
	 * get source record count from source
	 * @return record count
	 */
	public long getCount() {
		return this.getSourceRecordCount();
	}

	/**
	 * get schema(Metadata) corresponding to the data records
	 * @return schema
	 */
	public S getSchema() {
		return this.getOutputSchema();
	}

	/**
	 * get high watermark of the current pull
	 * @return high watermark
	 */
	public long getHighWatermark() {
		return this.highWatermark;
	}

	/**
	 * close extractor read stream
	 * update high watermark
	 */
	public void close() {
		this.workUnitState.setHighWaterMark(this.highWatermark);
	}
	
	/**
	 * @return full dump or not
	 */
	public boolean isFullDump() {
		return Boolean.valueOf(this.workUnit.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY));
	}

	/**
	 * build schema, record count and high water mark
	 */
	private void build() throws ExtractPrepareException {	
		String watermarkColumn = this.workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
		long lwm = this.workUnit.getLowWaterMark();
		long hwm = this.workUnit.getHighWaterMark();
		WatermarkType watermarkType;
		if(Strings.isNullOrEmpty(this.workUnit.getProp(ConfigurationKeys.SOURCE_WATERMARK_TYPE))) {
			watermarkType = null;
		} else {
			watermarkType = WatermarkType.valueOf(this.workUnit.getProp(ConfigurationKeys.SOURCE_WATERMARK_TYPE).toUpperCase());
		}
		
		if(!this.isValidWatermarkConfigs(watermarkColumn, watermarkType, lwm, hwm)) {
			throw new ExtractPrepareException("Watermark configs are not valid. Please verify watermarkColumn and watermark types");
		}	
		
		try {
			this.predicateColumnFormat = this.getWatermarkSourceFormat(watermarkType);
			
			this.setTimeOut(this.workUnit.getProp(ConfigurationKeys.SOURCE_TIMEOUT));
			
			this.extractMetadata(this.schema, this.entity, this.workUnit);
			
			this.highWatermark = this.getLatestWatermark(watermarkColumn, watermarkType, lwm, hwm, this.predicateColumnFormat);
			
			this.setRangePredicates(watermarkColumn, watermarkType, lwm, this.highWatermark);
			
			this.sourceRecordCount = this.getSourceCount(this.schema, this.entity, this.workUnit, this.predicateList);
		} catch (SchemaException e) {
			throw new ExtractPrepareException("Failed to get schema for this object; error-" + e.getMessage());
		} catch (HighWatermarkException e) {
			throw new ExtractPrepareException("Failed to get high watermark; error-" + e.getMessage());
		} catch (RecordCountException e) {
			throw new ExtractPrepareException("Failed to get record count; error-" + e.getMessage());
		} catch (Exception e) {
			throw new ExtractPrepareException("Failed to prepare the extract build; error-" + e.getMessage());
		}
	}

	/**
	 * verify the water mark related configs
     *
     * @param watermark column
     * @param watermark type
     * @param low watermark value
     * @param high watermark value
     * @return true if it is valid else false
	 */
	private boolean isValidWatermarkConfigs(String watermarkColumn, WatermarkType watermarkType, long lwm, long hwm) {
		if(lwm != DEFAULT_WATERMARK_VALUE || hwm != DEFAULT_WATERMARK_VALUE) {
			if(Strings.isNullOrEmpty(watermarkColumn) || watermarkType == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * if snapshot extract, get latest watermark else return work unit high watermark
     *
     * @param watermark column
     * @param low watermark value
     * @param high watermark value
     * @param column format
     * @return letst watermark
	 * @throws IOException 
	 */
	private long getLatestWatermark(String watermarkColumn, WatermarkType watermarkType, long lwmValue, long hwmValue, String predicateColumnFormat)
			throws HighWatermarkException, IOException {
		
		if(!Boolean.valueOf(this.workUnit.getProp(ConfigurationKeys.SOURCE_SKIP_HIGH_WATERMARK_CALC))) {
			LOG.info("Getting high watermark");
			List<Predicate> list = new ArrayList<Predicate>();
			Predicate lwmPredicate = this.getPredicate(watermarkColumn, watermarkType, lwmValue, ">=");
			Predicate hwmPredicate = this.getPredicate(watermarkColumn, watermarkType, hwmValue, "<=");
			if (lwmPredicate != null) {
				list.add(lwmPredicate);
			}
			if (hwmPredicate != null) {
				list.add(hwmPredicate);
			}

			return this.getMaxWatermark(this.schema, this.entity, watermarkColumn, predicateColumnFormat, list);
		}
		
		return hwmValue;
	}

	/**
	 * range predicates for watermark column and transaction columns.
	 * @param string 
	 * @param watermarkType 
     * @param watermark column
     * @param date column(for appends)
     * @param hour column(for appends)
     * @param batch column(for appends)
     * @param low watermark value
     * @param high watermark value
	 */
	private void setRangePredicates(String watermarkColumn, WatermarkType watermarkType, long lwmValue, long hwmValue) {
		LOG.info("Getting range predicates");
		Predicate lwmPredicate = this.getPredicate(watermarkColumn, watermarkType, lwmValue, ">=");
		Predicate hwmPredicate = this.getPredicate(watermarkColumn, watermarkType, hwmValue, "<=");
		if (lwmPredicate != null) {
			this.predicateList.add(lwmPredicate);
		}
		if (hwmPredicate != null) {
			this.predicateList.add(hwmPredicate);
		}
	}

	/**
	 * predicates using watermark column and transaction columns.
     * @param watermark column
     * @param date column(for appends)
     * @param hour column(for appends)
     * @param batch column(for appends)
     * @param low watermark value
     * @return predicate condition
	 */
	private Predicate getPredicate(String watermarkColumn, WatermarkType watermarkType, long value, String operator) {
		if(Strings.isNullOrEmpty(watermarkColumn) || watermarkType == null) {
			return null;
		}
		
		LOG.debug("Building predicate");
		String columnName = watermarkColumn;
		String condition = "";
		
		if(value != DEFAULT_WATERMARK_VALUE) {
			switch (watermarkType) {
			case TIMESTAMP:
				condition = this.getTimestampPredicateCondition(watermarkColumn, value, DEFAULT_WATERMARK_VALUE_FORMAT, operator);
				break;
			case DATE:
				condition = this.getDatePredicateCondition(watermarkColumn, value, DEFAULT_WATERMARK_VALUE_FORMAT, operator);
				break;
			case SIMPLE:
				condition = this.getSimplePredicateCondition(watermarkColumn, value, operator);
				break;
			default:
				condition = "";
				break;
			}
			
			
			if(Boolean.valueOf(this.workUnit.getProp(ConfigurationKeys.SOURCE_IS_HOURLY_EXTRACT))) {
				String hourCondition = this.getSimplePredicateCondition(this.workUnit.getProp(ConfigurationKeys.SOURCE_HOUR_COLUMN), this.extractFromWatermarkValue(Long.toString(value), "HH", ""),
						operator);
				if(condition.isEmpty()) {
					condition = hourCondition;
				} else {
					condition = condition + " AND " + hourCondition;
				}			
			}
		}
		
		if (Strings.isNullOrEmpty(columnName) || condition.equals("")) {
			return null;
		}
		
		return new Predicate(columnName, value, condition, this.predicateColumnFormat);
	}

	/**
	 * get date or hour from the given timestamp
	 */
	private long extractFromWatermarkValue(String input, String outfmt, String appendString) {
		Date date = null;
		try {
			date = DEFAULT_WATERMARK_TIMESTAMP_FORMAT.parse(input);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat outFormat = new SimpleDateFormat(outfmt);
		return Long.parseLong(outFormat.format(date) + appendString);
	}
	
	private String printTiming(long start, long end) {
		long totalMillis = end - start;
		long mins = TimeUnit.MILLISECONDS.toMinutes(totalMillis);
		long secs = TimeUnit.MILLISECONDS.toSeconds(totalMillis) - TimeUnit.MINUTES.toSeconds(mins);
		long millis = TimeUnit.MILLISECONDS.toMillis(totalMillis) - TimeUnit.MINUTES.toMillis(secs);
		return String.format("%d min, %d sec, %d millis", mins, secs, millis);
	}

	/**
	 * get column list from the user provided query to build schema with the respective columns
	 * @param input query
     * @return list of columns 
	 */
	protected List<String> getColumnListFromQuery(String query) {
		if (Strings.isNullOrEmpty(query)) {
			return null;
		}
		String queryLowerCase = query.toLowerCase();
		int startIndex = queryLowerCase.indexOf("select") + 6;
		int endIndex = queryLowerCase.indexOf("from ") - 1;
		String[] inputQueryColumns = query.substring(startIndex, endIndex).toLowerCase().replaceAll(" ", "").split(",");
		return Arrays.asList(inputQueryColumns);
	}

	/**
	 * True if the column is watermark column else return false
	 */
	protected boolean isWatermarkColumn(String watermarkColumn, String columnName) {
		if (columnName != null) {
			columnName = columnName.toLowerCase();
		}

		if (!Strings.isNullOrEmpty(watermarkColumn)) {
			List<String> waterMarkColumnList = Arrays.asList(watermarkColumn.toLowerCase().split(","));
			if (waterMarkColumnList.contains(columnName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Index of the primary key column from the given list of columns
	 */
	protected int getPrimarykeyIndex(String primarykeyColumn, String columnName) {
		if (columnName != null) {
			columnName = columnName.toLowerCase();
		}

		if (!Strings.isNullOrEmpty(primarykeyColumn)) {
			List<String> primarykeyColumnList = Arrays.asList(primarykeyColumn.toLowerCase().split(","));
			return primarykeyColumnList.indexOf(columnName) + 1;
		}
		return 0;
	}

	/**
	 * True if it is metadata column else return false
	 */
	protected boolean isMetadataColumn(String columnName, List<String> columnList) {
		columnName = columnName.trim().toLowerCase();
		if (columnList.contains(columnName)) {
			return true;
		}
		return false;
	}

	/**
	 * get required datatype from the given data type
	 */
	protected String getConvertedDataType(String dataType, Map<String, String> dataTypeMap) {
		if (dataType == null) {
			return "string";
		}
		String newDataType = dataTypeMap.get(dataType.toLowerCase());
		if (newDataType == null) {
			return "string";
		}
		return newDataType;
	}

	protected String toDateTimeFormat(String input, String inputfmt, String outputfmt) {
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
	
	protected JsonObject convertDataType(String columnName, String type, String elementType, List<String> enumSymbols) {
		String dataType = this.getDataTypeMap().get(type);
		if (dataType == null) {
			dataType = "string";
		}
		DataType convertedDataType;
		if (dataType.equals("map")) {
			convertedDataType = new MapDataType(dataType, elementType);
		} else if (dataType.equals("array")) {
			convertedDataType = new ArrayDataType(dataType, elementType);
		} else if (dataType.equals("enum")) {
			convertedDataType = new EnumDataType(dataType, columnName, enumSymbols);
		} else {
			convertedDataType = new DataType(dataType);
		}

		return gson.fromJson(gson.toJson(convertedDataType), JsonObject.class).getAsJsonObject();
	}
}
