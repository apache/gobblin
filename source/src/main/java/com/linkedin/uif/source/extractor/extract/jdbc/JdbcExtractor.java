package com.linkedin.uif.source.extractor.extract.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
import com.linkedin.uif.source.extractor.extract.SourceSpecificLayer;
import com.linkedin.uif.source.extractor.extract.jdbc.JdbcCommand.JdbcCommandType;
import com.linkedin.uif.source.extractor.resultset.RecordSetList;
import com.linkedin.uif.source.extractor.schema.ColumnMap;
import com.linkedin.uif.source.extractor.schema.Schema;
import com.linkedin.uif.source.extractor.utils.Utils;
import com.linkedin.uif.source.extractor.watermark.Predicate;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * Extract data using JDBC protocol
 * 
 * @author nveeramr
 */
public abstract class JdbcExtractor extends QueryBasedExtractor<JsonArray, JsonElement> implements SourceSpecificLayer<JsonArray, JsonElement>,
		JdbcSpecificLayer {
	private static final Gson gson = new Gson();
	private List<String> headerRecord;
	private boolean firstPull = true;
	private CommandOutput<?, ?> dataResponse = null;;
	protected String extractSql;
	protected long sampleRecordCount;
	protected JdbcProvider jdbcSource;
	protected int timeOut;
	private List<ColumnMap> columnAliasMap = new ArrayList<ColumnMap>();
	private Map<String, Schema> metadataColumnMap = new HashMap<String, Schema>();
	private List<String> metadataColumnList = new ArrayList<String>();
	private String inputColumnProjection;
	private String outputColumnProjection;
	private long totalRecordCount = 0;
	private boolean nextRecord = true;
	private int unknownColumnCounter = 1;

	/**
	 * Metadata column mapping to lookup columns specified in input query
	 * 
	 * @return metadata(schema) column mapping
	 */
	public Map<String, Schema> getMetadataColumnMap() {
		return metadataColumnMap;
	}

	/**
	 * @param metadata column mapping
	 */
	public void setMetadataColumnMap(Map<String, Schema> metadataColumnMap) {
		this.metadataColumnMap = metadataColumnMap;
	}

	/**
	 * Metadata column list
	 * 
	 * @return metadata(schema) column list
	 */
	public List<String> getMetadataColumnList() {
		return metadataColumnList;
	}

	/**
	 * @param metadata column list
	 */
	public void setMetadataColumnList(List<String> metadataColumnList) {
		this.metadataColumnList = metadataColumnList;
	}

	/**
	 * Sample Records specified in input query
	 * 
	 * @return sample record count
	 */
	public long getSampleRecordCount() {
		return sampleRecordCount;
	}

	/**
	 * @param sample record count
	 */
	public void setSampleRecordCount(long sampleRecordCount) {
		this.sampleRecordCount = sampleRecordCount;
	}

	/**
	 * query to extract data from data source
	 * 
	 * @return query
	 */
	public String getExtractSql() {
		return extractSql;
	}

	/**
	 * @param extract query
	 */
	public void setExtractSql(String extractSql) {
		this.extractSql = extractSql;
	}

	/**
	 * output column projection with aliases specified in input sql
	 * 
	 * @return column projection
	 */
	public String getOutputColumnProjection() {
		return outputColumnProjection;
	}

	/**
	 * @param output column projection
	 */
	public void setOutputColumnProjection(String outputColumnProjection) {
		this.outputColumnProjection = outputColumnProjection;
	}

	/**
	 * input column projection with source columns specified in input sql
	 * 
	 * @return column projection
	 */
	public String getInputColumnProjection() {
		return inputColumnProjection;
	}

	/**
	 * @param input column projection
	 */
	public void setInputColumnProjection(String inputColumnProjection) {
		this.inputColumnProjection = inputColumnProjection;
	}

	/**
	 * source column and alias mapping
	 * 
	 * @return map of column name and alias name
	 */
	public List<ColumnMap> getColumnAliasMap() {
		return columnAliasMap;
	}

	/**
	 * add column and alias mapping
	 * 
	 * @param column mapping
	 */
	public void addToColumnAliasMap(ColumnMap columnAliasMap) {
		this.columnAliasMap.add(columnAliasMap);
	}

	/**
	 * check whether is first pull or not
	 * 
	 * @return true, for the first run and it will be set to false after the
	 *         first run
	 */
	public boolean isFirstPull() {
		return firstPull;
	}

	/**
	 * @param firstPull
	 */
	public void setFirstPull(boolean firstPull) {
		this.firstPull = firstPull;
	}

	/**
	 * Header record to convert csv to json
	 * 
	 * @return header record with list of columns
	 */
	protected List<String> getHeaderRecord() {
		return headerRecord;
	}

	/**
	 * @param list of column names
	 */
	protected void setHeaderRecord(List<String> headerRecord) {
		this.headerRecord = headerRecord;
	}

	/**
	 * @return connection timeout
	 */
	public int getTimeOut() {
		return timeOut;
	}

	/**
	 * @return true, if records available. Otherwise, false
	 */
	public boolean hasNextRecord() {
		return nextRecord;
	}

	/**
	 * @param set next Record
	 */
	public void setNextRecord(boolean nextRecord) {
		this.nextRecord = nextRecord;
	}

	/**
	 * @param set connection timeout
	 */
	@Override
	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	public JdbcExtractor(WorkUnitState workUnitState) {
		super(workUnitState);
	}

	@Override
	public void extractMetadata(String schema, String entity, WorkUnit workUnit) throws SchemaException, IOException {
		this.log.info("Extract metadata using JDBC");
		String inputQuery = workUnit.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
		String watermarkColumn = workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
		JsonObject defaultWatermark = this.getDefaultWatermark();
		String defaultWatermarkColumnName = defaultWatermark.get("columnName").getAsString();
		this.setSampleRecordCount(this.exractSampleRecordCountFromQuery(inputQuery));
		JsonArray targetSchema = new JsonArray();
		List<String> headerColumns = new ArrayList<String>();

		try {
			List<Command> cmds = this.getSchemaMetadata(schema, entity);
			CommandOutput<?, ?> response = this.executePreparedSql(cmds);
			JsonArray array = this.getSchema(response);

			this.buildMetadataColumnMap(array);
			this.parseInputQuery(inputQuery);
			List<String> sourceColumns = this.getMetadataColumnList();

			for (ColumnMap colMap : this.columnAliasMap) {
				String alias = colMap.getAliasName();
				String sourceColumnName = colMap.getColumnName();
				if (this.isMetadataColumn(sourceColumnName, sourceColumns)) {
					String targetColumnName = this.getTargetColumnName(sourceColumnName, alias);
					Schema obj = this.getUpdatedSchemaObject(sourceColumnName, targetColumnName);
					String jsonStr = gson.toJson(obj);
					JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
					targetSchema.add(jsonObject);
					headerColumns.add(targetColumnName);
					this.columnList.add(sourceColumnName);
				}
			}

			if (this.hasMultipleWatermarkColumns(watermarkColumn)) {
				this.columnList.add(defaultWatermarkColumnName);
				headerColumns.add(defaultWatermarkColumnName);
				targetSchema.add(defaultWatermark);
			}

			String outputColProjection = Joiner.on(",").useForNull("null").join(this.columnList);
			outputColProjection = outputColProjection.replace(defaultWatermarkColumnName, getWatermarkColumnName(watermarkColumn) + " AS "
					+ defaultWatermarkColumnName);
			this.setOutputColumnProjection(outputColProjection);
			String extractQuery = this.getExtractQuery(schema, entity, inputQuery);

			this.setHeaderRecord(headerColumns);
			this.setOutputSchema(targetSchema);
			this.setExtractSql(extractQuery);
			//this.workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, this.escapeCharsInColumnName(this.workUnit.getProp(ConfigurationKeys.SOURCE_ENTITY), ConfigurationKeys.ESCAPE_CHARS_IN_COLUMN_NAME, "_"));
			this.log.info("Schema:" + targetSchema);
			this.log.info("Extract query: " + this.getExtractSql());
		} catch (Exception e) {
			throw new SchemaException("Failed to get metadata using JDBC; error - " + e.getMessage(), e);
		}
	}

	/**
	 * Build/Format input query in the required format
	 * 
	 * @param schema
	 * @param entity
	 * @param inputQuery
	 * @return formatted extract query
	 */
	private String getExtractQuery(String schema, String entity, String inputQuery) {
		String inputColProjection = this.getInputColumnProjection();
		String outputColProjection = this.getOutputColumnProjection();
		System.out.println("inputQuery:" + inputQuery);
		String query = this.removeSampleClauseFromQuery(inputQuery);
		System.out.println("query:" + query);
		if (query == null) {
			// if input query is null, build the query from metadata
			query = "SELECT " + outputColProjection + " FROM " + schema + "." + entity;
		} else {
			// replace input column projection with output column projection
			if (!Strings.isNullOrEmpty(inputColProjection)) {
				query = query.replace(inputColProjection, outputColProjection);
			}
		}

		String watermarkPredicateSymbol = ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL;
		if (!query.contains(watermarkPredicateSymbol)) {
			query = this.addPredicate(query, watermarkPredicateSymbol);
		}
		return query;
	}

	/**
	 * Update schema of source column Update column name with target column
	 * name/alias Update watermark, nullable and primary key flags
	 * 
	 * @param sourceColumnName
	 * @param targetColumnName
	 * @return schema object of a column
	 */
	private Schema getUpdatedSchemaObject(String sourceColumnName, String targetColumnName) {
		Schema obj = this.getMetadataColumnMap().get(sourceColumnName.toLowerCase());
		if (obj == null) {
			obj = getCustomColumnSchema(targetColumnName);
		} else {
			String watermarkColumn = workUnit.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
			String primarykeyColumn = workUnit.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
			boolean isMultiColumnWatermark = this.hasMultipleWatermarkColumns(watermarkColumn);

			obj.setColumnName(targetColumnName);
			boolean isWatermarkColumn = this.isWatermarkColumn(watermarkColumn, sourceColumnName);
			if (isWatermarkColumn) {
				this.updateDeltaFieldConfig(sourceColumnName, targetColumnName);
			}

			// If there is only one watermark column, then consider it as a
			// watermark. Otherwise add a default watermark column in the end
			if (!isMultiColumnWatermark) {
				obj.setWaterMark(isWatermarkColumn);
			}

			// override all columns to nullable except primary key and watermark
			// columns
			if ((isWatermarkColumn && !isMultiColumnWatermark) || this.getPrimarykeyIndex(primarykeyColumn, sourceColumnName) > 0) {
				obj.setNullable(false);
			} else {
				obj.setNullable(true);
			}

			// set primary key index for all the primary key fields
			int primarykeyIndex = this.getPrimarykeyIndex(primarykeyColumn, sourceColumnName);
			if (primarykeyIndex > 0 && (!sourceColumnName.equalsIgnoreCase(targetColumnName))) {
				this.updatePrimaryKeyConfig(sourceColumnName, targetColumnName);
			}

			obj.setPrimaryKey(primarykeyIndex);
		}
		return obj;
	}

	/**
	 * Get target column name if column is not found in metadata, then name it
	 * as unknown column If alias is not found, target column is nothing but
	 * source column
	 * 
	 * @param sourceColumnName
	 * @param alias
	 * @return targetColumnName
	 */
	private String getTargetColumnName(String sourceColumnName, String alias) {
		String targetColumnName = alias;
		Schema obj = this.getMetadataColumnMap().get(sourceColumnName.toLowerCase());
		if (obj == null) {
			targetColumnName = (targetColumnName == null ? "unknown" + this.unknownColumnCounter : targetColumnName);
			this.unknownColumnCounter++;
		} else {
			targetColumnName = (!Strings.isNullOrEmpty(targetColumnName) ? targetColumnName : sourceColumnName);
		}
		return Utils.escapeChars(targetColumnName, ConfigurationKeys.ESCAPE_CHARS_IN_COLUMN_NAME, "_");
	}

	/**
	 * Build metadata column map with column name and column schema object. 
	 * Build metadata column list with list columns in metadata
	 * 
	 * @param Schema of all columns
	 */
	private void buildMetadataColumnMap(JsonArray array) {
		if (array != null) {
			for (JsonElement columnElement : array) {
				Schema schemaObj = gson.fromJson(columnElement, Schema.class);
				String columnName = schemaObj.getColumnName();
				this.metadataColumnMap.put(columnName.toLowerCase(), schemaObj);
				this.metadataColumnList.add(columnName.toLowerCase());
			}
		}
	}

	/**
	 * Update water mark column property if there is an alias defined in query
	 * 
	 * @param source column name
	 * @param target column name
	 */
	private void updateDeltaFieldConfig(String srcColumnName, String tgtColumnName) {
		String watermarkCol = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
		this.workUnitState.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, watermarkCol.replaceAll(srcColumnName, tgtColumnName));
	}

	/**
	 * Update primary key column property if there is an alias defined in query
	 * 
	 * @param source column name
	 * @param target column name
	 */
	private void updatePrimaryKeyConfig(String srcColumnName, String tgtColumnName) {
		String primarykey = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
		this.workUnitState.setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, primarykey.replaceAll(srcColumnName, tgtColumnName));
	}

	/**
	 * If input query is null or '*' in the select list, consider all columns.
	 * 
	 * @return true, to select all colums. else, false.
	 */
	private boolean isSelectAllColumns() {
		String columnProjection = this.getInputColumnProjection();
		if (columnProjection == null || columnProjection.trim().equals("*")) {
			return true;
		}
		return false;
	}

	/**
	 * Parse query provided in pull file Set input column projection - column
	 * projection in the input query Set columnAlias map - column and its alias
	 * mentioned in input query
	 * 
	 * @param input query
	 */
	private void parseInputQuery(String query) {
		List<String> projectedColumns = new ArrayList<String>();
		if (!Strings.isNullOrEmpty(query)) {
			String queryLowerCase = query.toLowerCase();
			int startIndex = queryLowerCase.indexOf("select ") + 7;
			int endIndex = queryLowerCase.indexOf(" from ");
			if (startIndex >= 0 && endIndex >= 0) {
				String columnProjection = query.substring(startIndex, endIndex);
				this.setInputColumnProjection(columnProjection);
				projectedColumns = Arrays.asList(columnProjection.split(","));
			}
		}

		if (this.isSelectAllColumns()) {
			List<String> columnList = this.getMetadataColumnList();
			for (String columnName : columnList) {
				ColumnMap col = new ColumnMap();
				col.setColumnName(columnName);
				col.setAliasName(columnName);
				this.addToColumnAliasMap(col);
			}
		} else {
			for (String column : projectedColumns) {
				String sourceColumn = column.trim();
				String alias = null;
				int spaceOccurences = StringUtils.countMatches(sourceColumn.trim(), " ");
				if (spaceOccurences > 0) {
					// separate column and alias if they are separated by "as" or space
					String formattedColumn = sourceColumn.replaceAll(" as ", " ").replaceAll(" +", " ");
					int lastSpaceIndex = formattedColumn.lastIndexOf(" ");
					sourceColumn = formattedColumn.substring(0, lastSpaceIndex);
					alias = formattedColumn.substring(lastSpaceIndex + 1);
				}

				// extract column name if projection has table name in it
				if (sourceColumn.contains(".")) {
					sourceColumn = sourceColumn.substring(sourceColumn.indexOf(".") + 1);
				}

				ColumnMap col = new ColumnMap();
				col.setColumnName(sourceColumn);
				col.setAliasName(alias);
				this.addToColumnAliasMap(col);
			}
		}
	}

	/**
	 * Execute query using JDBC simple Statement Set fetch size
	 * 
	 * @param commands - query, fetch size
	 * @return JDBC ResultSet
	 */
	private CommandOutput<?, ?> executeSql(List<Command> cmds) {
		String query = null;
		int fetchSize = 0;

		for (Command cmd : cmds) {
			if (cmd instanceof JdbcCommand) {
				JdbcCommandType type = (JdbcCommandType) cmd.getCommandType();
				switch (type) {
				case QUERY:
					query = cmd.getParams().get(0);
					break;
				case FETCHSIZE:
					fetchSize = Integer.parseInt(cmd.getParams().get(0));
					break;
				default:
					log.error("Command " + type.toString() + " not recognized");
					break;
				}
			}
		}

		this.log.info("Executing query:" + query);
		ResultSet resultSet = null;
		try {
			this.jdbcSource = createJdbcSource();
			Connection connection = this.jdbcSource.getConnection();
			Statement statement = connection.createStatement();

			if (fetchSize != 0 && this.getExpectedRecordCount() > 2000) {
				statement.setFetchSize(fetchSize);
			}
			final boolean status = statement.execute(query);
			if (status == false) {
				log.error("Failed to execute sql:" + query);
			}
			resultSet = statement.getResultSet();
		} catch (Exception e) {
			log.error("Failed to execute sql:" + query + " ;error-" + e.getMessage(), e);
		}

		CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
		output.put((JdbcCommand) cmds.get(0), resultSet);
		return output;
	}

	/**
	 * Execute query using JDBC PreparedStatement to pass query parameters Set
	 * fetch size
	 * 
	 * @param commands - query, fetch size, query parameters
	 * @return JDBC ResultSet
	 */
	private CommandOutput<?, ?> executePreparedSql(List<Command> cmds) {
		String query = null;
		List<String> queryParameters = null;
		int fetchSize = 0;

		for (Command cmd : cmds) {
			if (cmd instanceof JdbcCommand) {
				JdbcCommandType type = (JdbcCommandType) cmd.getCommandType();
				switch (type) {
				case QUERY:
					query = cmd.getParams().get(0);
					break;
				case QUERYPARAMS:
					queryParameters = cmd.getParams();
					break;
				case FETCHSIZE:
					fetchSize = Integer.parseInt(cmd.getParams().get(0));
					break;
				default:
					log.error("Command " + type.toString() + " not recognized");
					break;
				}
			}
		}

		this.log.info("Executing query:" + query);
		ResultSet resultSet = null;
		try {
			this.jdbcSource = createJdbcSource();
			Connection connection = this.jdbcSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			int parameterPosition = 1;
			if (queryParameters != null && queryParameters.size() > 0) {
				for (String parameter : queryParameters) {
					statement.setString(parameterPosition, parameter);
					parameterPosition++;
				}
			}
			if (fetchSize != 0) {
				statement.setFetchSize(fetchSize);
			}
			final boolean status = statement.execute();
			if (status == false) {
				log.error("Failed to execute sql:" + query);
			}
			resultSet = statement.getResultSet();

		} catch (Exception e) {
			log.error("Failed to execute sql:" + query + " ;error-" + e.getMessage(), e);
		}

		CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
		output.put((JdbcCommand) cmds.get(0), resultSet);
		return output;
	}

	/**
	 * Create JDBC source to get connection
	 * 
	 * @return JDBCSource
	 */
	protected JdbcProvider createJdbcSource() {
		String driver = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_DRIVER);
		String userName = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
		String password = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD);
		String connectionUrl = this.getConnectionUrl();

		if (this.jdbcSource == null || this.jdbcSource.isClosed()) {
			this.jdbcSource = new JdbcProvider(driver, connectionUrl, userName, password, 1, this.getTimeOut());
			return this.jdbcSource;
		} else {
			return this.jdbcSource;
		}
	}

	@Override
	public long getMaxWatermark(String schema, String entity, String watermarkColumn, List<Predicate> predicateList, String watermarkSourceFormat)
			throws HighWatermarkException {
		this.log.info("Get high watermark using JDBC");
		long CalculatedHighWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

		try {
			List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, predicateList);
			CommandOutput<?, ?> response = this.executeSql(cmds);
			CalculatedHighWatermark = this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
			return CalculatedHighWatermark;
		} catch (Exception e) {
			throw new HighWatermarkException("Failed to get high watermark using JDBC; error - " + e.getMessage(), e);
		}
	}

	@Override
	public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws RecordCountException {
		this.log.info("Get source record count using JDBC");
		long count = 0;
		try {
			List<Command> cmds = this.getCountMetadata(schema, entity, workUnit, predicateList);
			CommandOutput<?, ?> response = this.executeSql(cmds);
			count = this.getCount(response);
			this.log.info("Source record count:" + count);
			return count;
		} catch (Exception e) {
			throw new RecordCountException("Failed to get source record count using JDBC; error - " + e.getMessage(), e);
		}
	}

	@Override
	public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
			throws DataRecordException, IOException {
		Iterator<JsonElement> rs = null;
		List<Command> cmds;
		try {
			if (isFirstPull()) {
				this.log.info("Get data recordset using JDBC");
				cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
				this.dataResponse = this.executePreparedSql(cmds);
				this.setFirstPull(false);
			}

			rs = this.getData(this.dataResponse);
			return rs;
		} catch (Exception e) {
			throw new DataRecordException("Failed to get record set using JDBC; error - " + e.getMessage(), e);
		}
	}

	/**
	 * Add a new predicate(filter condition) to the query
	 * 
	 * @param query
	 * @param predicate
	 * @return query
	 */
	protected String addPredicate(String query, String predicateCond) {
		String predicate = "where";
		if (query.toLowerCase().contains(predicate)) {
			predicate = "and";
		} else if (query.toLowerCase().contains(predicate)) {
			predicate = "and";
		}
		query = query + Utils.getClause(predicate, predicateCond);
		return query;
	}

	/**
	 * Get coalesce of water mark columns if there are multiple water mark columns
	 * 
	 * @param water mark column
	 * @return water mark column
	 */
	protected String getWatermarkColumnName(String waterMarkColumn) {
		int waterMarkColumnCount = 0;

		if (waterMarkColumn != null) {
			waterMarkColumnCount = Arrays.asList(waterMarkColumn.split(",")).size();
		}

		switch (waterMarkColumnCount) {
		case 0:
			return null;
		case 1:
			return waterMarkColumn;
		default:
			return "COALESCE(" + waterMarkColumn + ")";
		}
	}

	@Override
	public JsonArray getSchema(CommandOutput<?, ?> response) throws SchemaException, IOException {
		this.log.debug("Extract schema from resultset");
		ResultSet resultset = null;
		Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
		if (itr.hasNext()) {
			resultset = itr.next();
		} else {
			log.error("Failed to get schema from Mysql - Resultset has no records");
		}

		JsonArray fieldJsonArray = new JsonArray();
		try {
			while (resultset.next()) {
				Schema schema = new Schema();
				String columnName = resultset.getString(1);
				schema.setColumnName(columnName);

				String dataType = resultset.getString(2);
				String elementDataType = "string";
				List<String> mapSymbols = null;
				JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);

				schema.setDataType(newDataType);
				schema.setLength(resultset.getLong(3));
				schema.setPrecision(resultset.getInt(4));
				schema.setScale(resultset.getInt(5));
				schema.setNullable(resultset.getBoolean(6));
				schema.setFormat(resultset.getString(7));
				schema.setComment(resultset.getString(8));
				schema.setDefaultValue(null);
				schema.setUnique(false);

				String jsonStr = gson.toJson(schema);
				JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
				fieldJsonArray.add(obj);
			}
		} catch (Exception e) {
			throw new SchemaException("Failed to get schema from Mysql; error - " + e.getMessage(), e);
		}

		return fieldJsonArray;
	}

	@Override
	public long getHighWatermark(CommandOutput<?, ?> response, String watermarkColumn, String watermarkColumnFormat) throws HighWatermarkException {
		this.log.debug("Extract high watermark from resultset");
		ResultSet resultset = null;
		Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
		if (itr.hasNext()) {
			resultset = itr.next();
		} else {
			log.error("Failed to get high watermark from Mysql - Resultset has no records");
		}

		Long HighWatermark;
		try {
			String watermark;
			if (resultset.next()) {
				watermark = resultset.getString(1);
			} else {
				watermark = null;
			}

			if (watermark == null) {
				return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
			}

			if (watermarkColumnFormat != null) {
				SimpleDateFormat inFormat = new SimpleDateFormat(watermarkColumnFormat);
				Date date = null;
				try {
					date = inFormat.parse(watermark);
				} catch (ParseException e) {
					log.error("ParseException: " + e.getMessage(), e);
				}
				SimpleDateFormat outFormat = new SimpleDateFormat("yyyyMMddHHmmss");
				HighWatermark = Long.parseLong(outFormat.format(date));
			} else {
				HighWatermark = Long.parseLong(watermark);
			}

		} catch (Exception e) {
			throw new HighWatermarkException("Failed to get high watermark from Mysql; error - " + e.getMessage(), e);
		}

		return HighWatermark;
	}

	@Override
	public long getCount(CommandOutput<?, ?> response) throws RecordCountException {
		this.log.debug("Extract source record count from resultset");
		ResultSet resultset = null;
		Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
		if (itr.hasNext()) {
			resultset = itr.next();
		} else {
			log.error("Failed to get source record count from Mysql - Resultset has no records");
		}

		long count = 0;
		try {
			if (resultset.next()) {
				count = resultset.getLong(1);
			}
		} catch (Exception e) {
			throw new RecordCountException("Failed to get source record count from MySql; error - " + e.getMessage(), e);
		}

		return count;
	}

	@Override
	public Iterator<JsonElement> getData(CommandOutput<?, ?> response) throws DataRecordException, IOException {
		this.log.debug("Extract data records from resultset");
		RecordSetList<JsonElement> recordSet = this.getNewRecordSetList();

		if (response == null || !this.hasNextRecord()) {
			return recordSet.iterator();
		}

		ResultSet resultset = null;
		Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
		if (itr.hasNext()) {
			resultset = itr.next();
		} else {
			log.error("Failed to get source record count from Mysql - Resultset has no records");
		}

		try {
			final ResultSetMetaData resultsetMetadata = resultset.getMetaData();
			final int columnCount = resultsetMetadata.getColumnCount();

			int batchSize = Utils.getAsInt(this.workUnit.getProp(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE));
			batchSize = (batchSize == 0 ? ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE : batchSize);

			int recordCount = 0;
			while (resultset.next()) {
				List<String> record = new ArrayList<String>();
				for (int i = 1; i <= columnCount; i++) {
					record.add(resultset.getString(i));
				}

				JsonObject jsonObject = Utils.csvToJsonObject(this.getHeaderRecord(), record, columnCount);
				recordSet.add(jsonObject);
				recordCount++;
				this.totalRecordCount++;

				// Insert records in record set until it reaches the batch size
				if (recordCount >= batchSize) {
					this.log.info("Total number of records processed so far: " + this.totalRecordCount);
					return recordSet.iterator();
				}
			}
			this.setNextRecord(false);
			this.log.info("Total number of records processed so far: " + this.totalRecordCount);
			return recordSet.iterator();
		} catch (Exception e) {
			throw new DataRecordException("Failed to get records from MySql; error - " + e.getMessage(), e);
		}
	}

	protected static Command getCommand(String query, JdbcCommandType commandType) {
		return new JdbcCommand().build(Arrays.asList(query), commandType);
	}

	protected static Command getCommand(int fetchSize, JdbcCommandType commandType) {
		return new JdbcCommand().build(Arrays.asList(Integer.toString(fetchSize)), commandType);
	}

	protected static Command getCommand(List<String> params, JdbcCommandType commandType) {
		return new JdbcCommand().build(params, commandType);
	}

	/**
	 * Concatenate all predicates with "and" clause
	 * 
	 * @param list of predicate(filter) conditions
	 * @return predicate
	 */
	protected String concatPredicates(List<Predicate> predicateList) {
		List<String> conditions = new ArrayList<String>();
		Iterator<Predicate> i = predicateList.listIterator();
		while (i.hasNext()) {
			Predicate predicate = i.next();
			conditions.add(predicate.getCondition());
		}
		return Joiner.on(" and ").skipNulls().join(conditions);
	}

	/**
	 * Schema of default watermark column-required if there are multiple watermarks
	 * 
	 * @return column schema
	 */
	private JsonObject getDefaultWatermark() {
		Schema schema = new Schema();
		String dataType;
		String columnName = "DefaultWatermarkColumn";

		schema.setColumnName(columnName);

		WatermarkType wmType = WatermarkType.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "TIMESTAMP")
				.toUpperCase());
		switch (wmType) {
		case TIMESTAMP:
			dataType = "timestamp";
			break;
		case DATE:
			dataType = "date";
			break;
		default:
			dataType = "int";
			break;
		}

		String elementDataType = "string";
		List<String> mapSymbols = null;
		JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);
		schema.setDataType(newDataType);
		schema.setWaterMark(true);
		schema.setPrimaryKey(0);
		schema.setLength(0);
		schema.setPrecision(0);
		schema.setScale(0);
		schema.setNullable(false);
		schema.setFormat(null);
		schema.setComment("Default watermark column");
		schema.setDefaultValue(null);
		schema.setUnique(false);

		String jsonStr = gson.toJson(schema);
		JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
		return obj;
	}

	/**
	 * Schema of a custom column - required if column not found in metadata
	 * 
	 * @return column schema
	 */
	private Schema getCustomColumnSchema(String columnName) {
		Schema schema = new Schema();
		String dataType = "string";
		schema.setColumnName(columnName);
		String elementDataType = "string";
		List<String> mapSymbols = null;
		JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);
		schema.setDataType(newDataType);
		schema.setWaterMark(false);
		schema.setPrimaryKey(0);
		schema.setLength(0);
		schema.setPrecision(0);
		schema.setScale(0);
		schema.setNullable(true);
		schema.setFormat(null);
		schema.setComment("Custom column");
		schema.setDefaultValue(null);
		schema.setUnique(false);
		return schema;
	}

	/**
	 * New record set for iterator
	 * 
	 * @return RecordSetList
	 */
	private RecordSetList<JsonElement> getNewRecordSetList() {
		return new RecordSetList<JsonElement>();
	}

}
