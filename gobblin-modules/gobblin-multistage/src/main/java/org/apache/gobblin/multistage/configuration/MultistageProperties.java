/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.multistage.configuration;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.State;


/**
 * Each item is associated with a Java class so that its actual configuration value
 * can be validated. <p>
 *
 * The getProp() function will return values with the specified type, with default values. getProp
 * can raise exceptions if the configured value mismatch the specified type. For example, if a
 * string is configured for an Integer property, or an incorrect string is configured for for
 * a JsonObject property.<p>
 *
 * The getValidNonblankWithDefault() function will always return a validate value. And if the configured
 * value is not valid, a default value will be returned.
 *
 * The getMillis() function is mostly used to convert days, seconds, and other configured time values
 * to milli-seconds. This function might be meaningless to some properties. The default implementation
 * will return 0L.
 *
 * Each item can define functions like:<p>
 * - value validation, by implementing the validate() function for each property<p>
 * - conversion functions applicable to specific property types, like days to millis<p>
 * - provide default values, like default status codes<p>
 *
 * @author chrli
 */
@Slf4j
@Getter
@SuppressWarnings("unchecked")
public enum MultistageProperties {
  /**
   * Abstinent Period is designed to avoid re-extract a dataset repeatedly. This is particular useful
   * for situations like downloading files in large quantity.
   *
   * Assuming we will control all data extraction through a time range, including file downloads. Further
   * assuming that files were all uploaded to source on 6/30, and assuming that we can only download 100 files
   * a day, and there are 1000 files. Files downloaded on 7/1 will be downloaded again on 7/2 because
   * their cut off time is 7/1, which is before the new extraction time.
   *
   * An abstinent period 30 is thus added to the last download/extract time, allowing us move the cutoff time forward.
   * Therefore, if there is an abstinent period of 30 days, the downloaded files will not be downloaded
   * again in 30 days.
   *
   * Abstinent period can be set to a large number so that the same file will never be downloaded again.
   */
  MSTAGE_ABSTINENT_PERIOD_DAYS("ms.abstinent.period.days", Integer.class) {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * (Integer) this.getProp(state);
    }
  },
  /**
   * activation.property is a row object in JsonObject string format, the activation
   * property can container key value pairs to be used as filters or URI parameters
   * in extractor.
   *
   */
  MSTAGE_ACTIVATION_PROPERTY("ms.activation.property", JsonObject.class) {
    // this property is normally set from the Source and used in Extractor
    // therefore, we use Work Unit State to retrieve the value, and using
    // Source State to retrieve the value will always be blank.
    @Override
    public boolean validate(State state) {
      try {
        JsonObject activation = getProp(state);
        return activation == null || activation.entrySet().size() >= 0;
      } catch (Exception e) {
        return false;
      }
    }
  },
  MSTAGE_AUTHENTICATION("ms.authentication", JsonObject.class) {
    @Override
    // accepts only validly formed values
    public boolean validateNonblank(State state) {
      try {
        JsonObject auth = getProp(state);
        return auth.entrySet().size() > 0 && auth.has("method") && auth.has("encryption");
      } catch (Exception e) {
        return false;
      }
    }
  },
  /**
   * to do back fill, set ms.backfill=true, and also set the watermark
   * any value other than true indicates a normal load.
   *
   */
  MSTAGE_BACKFILL("ms.backfill", Boolean.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) Boolean.FALSE;
    }
  },
  /**
   * call.interval is used in pagination and waiting/looping
   *
   * when used in pagination, call.interval specify how long the client should wait
   * before submit a new page request.
   *
   * when used in waiting/looping, call.interval specify the waiting period between
   * calls.
   *
   * this value is in milliseconds.
   */
  MSTAGE_CALL_INTERVAL("ms.call.interval.millis", Long.class),
  MSTAGE_CSV_COLUMN_HEADER("ms.csv.column.header", Boolean.class),
  /**
   * a comma-separated string, where each value is either an integer or a range
   * representing the index of the field to include
   * Valid values include [0 based indexing]:
   * 0,1,2,3,4
   * 0,1,2,4-15
   * 0,1,3-7,10
   * 0,5,3-4,2
   *
   * Note: the values need not to be ordered
   */
  MSTAGE_CSV_COLUMN_PROJECTION("ms.csv.column.projection", String.class) {
    @Override
    // accepts only validly formed values
    public boolean validateNonblank(State state) {
      String columnProjections = getProp(state);
      return columnProjections != null && columnProjections.split(",").length > 0;
    }
  },
  MSTAGE_CSV_ESCAPE_CHARACTER("ms.csv.escape.character", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "u005C";
    }
  },
  MSTAGE_CSV_QUOTE_CHARACTER("ms.csv.quote.character", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "\"";
    }
  },
  MSTAGE_CSV_SEPARATOR("ms.csv.separator", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) ",";
    }
  },
  /**
   * if csv.column.header is true, csv.skip.lines will be 1 by default, if more than 1
   * row to be skipped, then set this parameter explicitly.
   *
   * if csv.column.header is false, csv.skip.lines will be 0 by default, if there are
   * rows to be skipped, then set this parameter explicitly.
   */
  MSTAGE_CSV_SKIP_LINES("ms.csv.skip.lines", Integer.class),
  MSTAGE_DATA_DEFAULT_TYPE("ms.data.default.type", JsonObject.class),
  /**
   * DATA_FIELD specified where true data payload is in a nested structure.
   * If data.field is not specified or it is blank, then the whole response will be
   * treated as the payload. If data.field is not present in the response,
   * it will generate an error.
   */
  MSTAGE_DATA_FIELD("ms.data.field", String.class),
  /**
   * derived.fields is an array of field definitions
   *
   * each field definition will have "name", "type", "source", and "format"
   *
   * Example 1: following define a derived field using regular expression to subtract part of a source field
   * [{
   *   "name": "surveyid",
   *   "formula": {
   *     "type": "regexp",
   *     "source": "survey_url",
   *     "format": "https.*\\/surveys\\/([0-9]+)$"
   *     }
   * }]
   *
   * Example 2: following define a epoc timestamp field to meet Lumos requirement
   * [{
   *   "name": "callDate",
   *   "formula": {
   *     "type": "epoc",
   *     "source": "started",
   *     "format": "yyyy-MM-dd"
   *   }
   * }]
   *
   */
  MSTAGE_DERIVED_FIELDS("ms.derived.fields", JsonArray.class) {
    @Override
    // accepts blank and validly formed values, only rejects badly formed values
    public boolean validate(State state) {
      JsonArray derivedFields = getProp(state);
      return derivedFields == null
          || derivedFields.size() == 0
          || validateNonblank(state);
    }
    @Override
    // accepts only validly formed values
    public boolean validateNonblank(State state) {
      JsonArray derivedFields = getProp(state);
      return derivedFields != null
          && derivedFields.size() > 0
          && derivedFields.get(0).getAsJsonObject().has("name")
          && derivedFields.get(0).getAsJsonObject().has("formula");
    }
  },
  /**
   * In this job property you can specify the fields (array of fields) which needs to be encrypted by the Gobblin
   * utility.
   * These fields can be of JsonPrimitive type (string/int/boolean/etc.) or JsonObject type (with nested structure)
   * e.g. "ms.encryption.fields" : ["emailAddress", "settings.webConferencesRecorded"]
   */
  MSTAGE_ENCRYPTION_FIELDS("ms.encryption.fields", JsonArray.class),
  /**
   * Limited cleansing include tasks such as standardizing element name and
   * replacing null values with default ones or dummy values
   *
   * Limited cleansing can also be used to replace certain elements in Json data.
   * Currently white spaces and $ in Json element names will be replaced with _ if
   * cleansing is enabled.
   *
   * Default: true
   *
   * Default value is used when this parameter is blank.
   *
   * This feature should be used only on need basis in large datasets where cleansing is expensive,
   * for example, where source data element names are un-conforming, such as containing spaces,
   * and needed standardization.
   *
   */
  MSTAGE_ENABLE_CLEANSING("ms.enable.cleansing", Boolean.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) Boolean.TRUE;
    }
  },

  /**
   * Dynamic full load will ignore extract.is.full setting and set extract.is.full based on following
   * condidtions:
   * 1. true if it is SNAPSHOT_ONLY extract
   * 2. true if there is no pre-existing watermarks of the job
   *
   * To observe the extract.is.full setting, disable dynamic full load
   */
  MSTAGE_ENABLE_DYNAMIC_FULL_LOAD("ms.enable.dynamic.full.load", Boolean.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) Boolean.TRUE;
    }
  },
  /**
   * each Extractor will enforce a compliance filter based on given schema, currently this is
   * soft enforced. Use case can turn the filter off by setting this parameter to false
   */
  MSTAGE_ENABLE_SCHEMA_BASED_FILTERING("ms.enable.schema.based.filtering", Boolean.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) Boolean.TRUE;
    }
  },
  MSTAGE_ENCODING("ms.encoding", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "UTF-8";
    }
  },
  /**
   * extract.preprocessors define one or more preprocessor classes
   */
  MSTAGE_EXTRACT_PREPROCESSORS("ms.extract.preprocessors", String.class),
  /**
   * Parameters to pass into the preprocessor along with the input.
   * e.g, If a source file is encrypted, it requires additional credentials to decrypt
   * For GPG based decryption/encryption, parameters follow {@link org.apache.gobblin.crypto.EncryptionConfigParser}
   * A sample parameter map:
   * {
   *   "action" : string, decrypt/encrypt
   *   "keystore_password" : string, some password,
   *   "keystore_path" : string, path to the secret keyring,
   *   "cipher" : string, optional, cipher algorithm to use, default to CAST5 (128 bit key, as per RFC 2144)
   *   "key_name" : string, optional, public key name needed for encryption
   * }
   */
  MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS("ms.extract.preprocessor.parameters", JsonObject.class),

  MSTAGE_EXTRACTOR_CLASS("ms.extractor.class", String.class),

  MSTAGE_EXTRACTOR_TARGET_FILE_NAME("ms.extractor.target.file.name", String.class),
  //use this property for file dump extractor to save file with specific permission.
  MSTAGE_EXTRACTOR_TARGET_FILE_PERMISSION("ms.extractor.target.file.permission", String.class) {
    @Override
    public <T> T getValidNonblankWithDefault(State state) {
      return (T) ((validateNonblank(state))
          ? ((String) getProp(state)).toUpperCase() : "755");
    }
  },
  /**
   * Grace Period is for overlapped data extraction, it assumes that the source can have late comers,
   * which are older data that showed up in source after last extract. For example, a record was modified
   * 2 days ago, but did not show up until today. In such case, if we extract based records' last
   * update date, the last extraction would have missed that record, amd today's extraction will
   * again miss it if we cut off by last extraction time (yesterday).
   *
   * A grace period is thus subtracted from the last extraction time, allowing us move the cut off
   * time backward. Therefore, if there is grace period of 2 days, it will capture data arrived 2 days
   * late in source.
   */
  MSTAGE_GRACE_PERIOD_DAYS("ms.grace.period.days", Integer.class) {
    @Override
    public Long getMillis(State state) {
      return 24L * 3600L * 1000L * (Integer) this.getProp(state);
    }
  },
  /**
   * http.client.factory define an indirect way to specify the type of HttpClient to use.
   * default = {@link org.apache.gobblin.multistage.factory.ApacheHttpClientFactory}
   * local testing = {@link org.apache.gobblin.multistage.factory.ApacheHttpClientFactory}
   */
  MSTAGE_HTTP_CLIENT_FACTORY("ms.http.client.factory", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "org.apache.gobblin.multistage.factory.ApacheHttpClientFactory";
    }
  },
  /**
   * custom headers include Content-Type are to be included in this property
   */
  MSTAGE_HTTP_REQUEST_HEADERS("ms.http.request.headers", JsonObject.class),
  MSTAGE_HTTP_REQUEST_METHOD("ms.http.request.method", String.class),
  /**
   * http.statuses defines success codes and warnings, and optionally errors.
   * By default, if this parameter is not set, 200 (OK), 201 (CREATED), and 202 (ACCEPTED)
   * will be treated as success; anything else below 400 will be treated as warning; and
   * anything 400 and above will be treated as error. Warnings will be logged but will not
   * cause job failure. Errors will cause job failure.
   *
   * In cases where 4xx codes, like 404 (NOT FOUND), happened frequently, and a failure is
   * not desirable, exceptions can be added to warnings.
   *
   * In following configuration, we make 404 an warning, and make 206 a failure indicating
   * that partial content is not acceptable:
   * {"success": [200], "warning": [404], "error": [206]}
   */
  MSTAGE_HTTP_STATUSES("ms.http.statuses", JsonObject.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) GSON.fromJson("{\"success\":[200,201,202], \"pagination_error\":[401]}", JsonObject.class);
    }
  },
  /**
   * http.status.reasons define reason codes (strings) that have special meaning in determining
   * whether a request was a success or failure.
   *
   * for example, when status is 200, but there is a reason to indicate the request was not successful,
   * then the status.reason can be set:
   * {"error": ["not found"]}
   *
   * An Http response is considered success if and only if:
   * - status code in http.statuses.success
   * - reason code not in http.status.reasons.error
   *
   * Currently, we don't allow exceptions being made to revert errors by using reason code.
   */
  MSTAGE_HTTP_STATUS_REASONS("ms.http.status.reasons", JsonObject.class),
  /**
   * JDBC statement is the query to be executed for data extraction, usually a SELECT
   * statement or a store procedure. DIL doesn't explicitly restrict or support syntax
   * of the statement. The source database decides whether to accept or fail the statement.
   */
  MSTAGE_JDBC_STATEMENT("ms.jdbc.statement", String.class),
  MSTAGE_OUTPUT_SCHEMA("ms.output.schema", JsonArray.class),
  /**
   * pagination is a Json object with 2 members:
   *
   * fields: is an array of up to 3 string elements, each denote a source key column for:
   *   1. page start, or offset
   *   2. page size, or limit of each page
   *   3. page no, if page no is used to control instead of using page start and page size
   *
   * initialvalues: is an array of up to 3 integer elements, each denote a initial value for:
   *   1. page start, or offset
   *   2. pagesize, or limit of each page
   *   3. page no, if page no is used to control instead of using page start and page size
   */
  MSTAGE_PAGINATION("ms.pagination", JsonObject.class),
  /**
   * ms.parameter holds a list of parameters in the form of a JsonArray.
   *
   * Parameters are named, i.e. the name will be referenced in other places.
   *
   * Parameters will have either static values or dynamic values derived from a formula.
   *
   * Terminology: in following description, we call parameters used in URI as URI Parameters.
   *
   * For HTTP GET requests, parameters will be used to form the final URI. In such case,
   * the parameter can be used in the URI path or as URI parameter.
   *
   * When used in URI path, the parameter name need to be specified in URI template
   * as a variable contained in {{}}.
   *
   * When used as URI parameters, the parameter name and derived value will be coded as
   * KV pairs; therefore, the parameter name need to be acceptable to source.
   *
   * For example, if a source accepts URI like http://domainname/endpoint?cursor=xxxx,
   * and the "cursor" parameter is optional, then the parameter should be named as
   * "cursor", and the URI template should be set as http://domainname/endpoint in pull file.
   * In such case, a "?cursor=xxx" will be appended to the final URI when cursor is
   * present.
   *
   * However, if the cursor URI parameter is not optional, the URI template could be coded as
   * http://domain/endpoint?cursor={{p1}}, then the parameter can be named as "p1", and the
   * parameter value will replace {{p1}} before the request is sent to the URI source.
   *
   * Examples of setting parameters in pull files:
   *
   * For one case, the URI needs 3 mandatory variables, and they can be named as p1, p2, and p3.
   * And we can configure the pull file as following:
   *
   * ms.uri=https://domain.com/api/bulk/2.0/syncs/{{p1}}/data?offset={{p2}}&limit={{p3}}
   * ms.parameter=[
   *   {"name": "p1", "type": "list", "value": "3837498"},
   *   {"name": "p2", "type": "pagestart"},
   *   {"name": "p3", "type": "pagesize"}]
   *
   *
   * For another case, the URI needs 1 optional variable, and the parameter has to be named as
   * required by the source. And here is the configuration:
   *
   * ms.uri=https://domain.com/users
   * ms.parameter=[{"name":"cursor","type":"session"}]
   *
   * For HTTP POST and HTTP PUT requests, the parameter name will be used as-is in the form of
   * "parameter name": "parameter value" in the request body; therefore, the parameter name
   * need to be as required by to URI source.
   *
   */

  MSTAGE_PARAMETERS("ms.parameters", JsonArray.class) {
    @Override
    public boolean validateNonblank(State state) {
      try {
        return ((JsonArray) getProp(state)).size() > 0;
      } catch (Exception e) {
        return false;
      }
    }
  },
  MSTAGE_RETENTION("ms.retention", JsonObject.class) {
    @Override
    public <T> T getDefaultValue() {
      JsonObject retention = new JsonObject();
      retention.addProperty("state.store", "P90D"); // keep 90 days state store by default
      retention.addProperty("publish.dir", "P731D"); // keep 2 years published data
      retention.addProperty("log", "P30D");
      return (T) retention;
    }
  },
  /**
   * This property is used to set: <p>
   * 1. location from where the hdfs data will be loaded as secondary data to call the
   * subsequent API <p>
   * 2. define the field names that needs to be extracted and added into the work units.
   * 3. define filters on one or more fields based on following rules
   *  a. if multiple fields are filtered, the relationship is AND, that means all condition must be met
   *  b. if a filter is defined on a field, and field value is NULL, the record is rejected
   *  c. if a filter is defined on a field, and the field value is not NULL, the record will be rejected if
   *     its value doesn't match the pattern
   *  d. if no filter is defined on a field, the default filter ".*" is applied to the field, and NULL values
   *     are accepted
   * 4. define the category of the input, currently we allow these categories:
   *  a. activation, that means the secondary input is for creating work units
   *  b. authentication, that means the secondary input provide authentication information
   *<p>
   *
   * Example :
   *
   *  ms.secondary.input=[{
   *  "path": "/path/to/hdfs/inputFileDir/2019/08/07/19/720",
   *  "fields": ["id", "tempId"],
   *  "filters": {"status": "(OK|Success)", "field2": "pattern2"},
   *  "category" "activation"
   *  }]
   *
   * The gobblin job will read records from that location and extract the two fields and inject it into the work units.
   */
  MSTAGE_SECONDARY_INPUT("ms.secondary.input", JsonArray.class) {
    @Override
    public boolean validate(State state) {
      return getProp(state) != null;
    }
  },
  /**
   * session.key.field specifies the key field for session and the condition for termination.
   * Although Restful API is stateless, data sources can maintain a session in backend
   * by a status field, a session cursor, or through pagination (see comments on PAGINATION).
   *
   * it takes the form a Json object with a "name" and a "condition". the name specifies
   * the field in response that gives session info. And the condition, specifies when the
   * session should stop. A condition can be a regular expression or a formula. Currently,
   * only regular expression is supported.
   *
   * "name" is required
   * "condition" is optional
   *
   * When both session and pagination are enabled, the extractor will keep consuming data from
   * source until all pages are extracted. Then the extractor will check the status until
   * the stop condition is met.
   *
   * In that regard, when the source give conflicting signal in turns of total expected rows
   * and status, the data can have duplicate, and actual extracted rows in log file should
   * show more rows extracted than expected.
   *
   */
  MSTAGE_SESSION_KEY_FIELD("ms.session.key.field", JsonObject.class),
  /**
   * Default source data character set is UTF-8, which should be good for most use cases.
   * See StandardCharsets for other common names, such as UTF-16
   */
  MSTAGE_SOURCE_DATA_CHARACTER_SET("ms.source.data.character.set", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "UTF-8";
    }
  },
  MSTAGE_SOURCE_FILES_PATTERN("ms.source.files.pattern", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) ".*";
    }
  },

  /**
   * Parameters specific to the S3 source.
   * {
   * "region": string, aws region code: https://docs.aws.amazon.com/general/latest/gr/rande.html
   * "read_timeout_seconds", integer, read time out in seconds
   * "write_timeout_seconds", integer, write time out in seconds
   * "connection_timeout_seconds": Sets the socket to timeout after failing to establish a connection with the server after milliseconds.
   * "connection_max_idle_millis",  Sets the socket to timeout after timeout milliseconds of inactivity on the socket.
   * }
   */
  MSTAGE_SOURCE_S3_PARAMETERS("ms.source.s3.parameters", JsonObject.class),

  /**
   * ms.source.uri defines a data source identifier, it follows the URI format
   * here: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
   *
   * The only exception is that authority is not supported, because all authority
   * cannot be fit in the URI.
   *
   * source.uri also accepts variables that allow substitution in runtime
   * Examples:
   *   ms.source.uri=https://abc.com/api/bulk/2.0/syncs/{{synid}}
   *   ms.source.uri=jdbc:mysql://abc.com:3630/abc?useSSL=true
   *   ms.source.uri=https://commoncrawl.s3.amazonaws.com/{{s3key}}
   *
   * TODO SFTP protocol to support URI format
   */
  MSTAGE_SOURCE_URI("ms.source.uri", String.class),

  // TODO: Merge back to @link{MSTAGE_SOURCE_S3_PARAMETERS}
  MSTAGE_S3_LIST_MAX_KEYS("ms.s3.list.max.keys", Integer.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) new Integer(1000);
    }
  },
  /**
   * Total count field is a Json path. This attribute can be used in many
   * types of connectors, typically with Json Extractor
   *
   * If response is like  { "records": { "totalRecords": 10000, "pagesize": 100, "currentpage": 0}},
   * the configurations should be:  ms.totalcount.field=records.totalRecords
   *
   */
  MSTAGE_TOTAL_COUNT_FIELD("ms.total.count.field", String.class) {
    @Override
    public boolean validateNonblank(State state) {
      String tcField = getProp(state);
      return StringUtils.isNotBlank(tcField);
    }
  },
  /**
   * If there is not total expected row count, the session will keep looping and waiting
   * until either the session completion condition is met or time out.
   *
   * wait.timeout control how long the job will wait before the session completion status
   * is met.
   *
   * default is 10 minutes or 600 seconds
   *
   * see also call.interval
   *
   */

  MSTAGE_WAIT_TIMEOUT_SECONDS("ms.wait.timeout.seconds", Long.class) {
    @Override
    public Long getMillis(State state) {
      return 1000L * (Long) this.getValidNonblankWithDefault(state);
    }

    @Override
    public <T> T getDefaultValue() {
      return (T) new Long(600);
    }
  },
  /**
   * ms.watermark holds a list of watermark ranges in the form of a JsonArray.
   * A watermark property is a JsonObject with name, type, and range.
   * For now, only datetime and unit type watermark are supported.
   *
   * For datetime watermark, a range has "from" and "to" values.
   * They have to be in "yyyy-MM-dd" format; however "to" can be just "-" to present current date.
   *
   * For example:
   *
   * ms.watermark=[{"name": "system","type": "datetime","range": {"from": "2019-01-01", "to": "-"}}]
   *
   */
  MSTAGE_WATERMARK("ms.watermark", JsonArray.class),
  MSTAGE_WATERMARK_GROUPS("ms.watermark.groups", JsonArray.class),
  MSTAGE_WORK_UNIT_PARALLELISM_MAX("ms.work.unit.parallelism.max", Integer.class) {
    @Override
    public boolean validateNonblank(State state) {
      Integer parallelMax = getProp(state);
      return parallelMax > 0;
    }
  },
  /**
   * Work unit paritioning scheme is either a string or a JsonObject.
   *
   * When it is a string, it will accept values like monthly, weekly, daily, hourly, or none,
   * which can be blank or literally "none".
   *
   * When it is a JsonObject, there can be multiple ways to partition, either with a range. For
   * example, following will break 2010-2019 by monthly partitions, and daily partitions afterwards.
   *
   * {"monthly": ["2010-01-01", "2020-01-01"], "daily": ["2020-01-01": "-"]}
   *
   * In such case, the partition is called composite. For the composite partition to work,
   * the ranges should be continuous with no gaps or overlaps. In order to avoid gaps and overlaps,
   * one range end should be the same as another range's start.
   *
   * Note the end of partition accepts "-" as current date, but it doesn't access PxD syntax, the
   * reason being a partition range can be broader than watermark range.
   *
   * For a composite partition, if the range definition is not as specified, or not valid, then the there
   * is no partitioning, equivalent to ms.work.unit.partition=''
   *
   * For a composite partition, a range is matched against watermark to define partitions, if a range
   * is smaller than full partition range, for example {"monthly": ["2020-01-01", "2020-01-18"]},
   * it will still generate a full partition. So to avoid confusion, the range should be at minimum
   * 1 partition size. That means, a range should at least 1 month for monthly, or at least 1 week for
   * etc.
   *
   */
  MSTAGE_WORK_UNIT_PARTITION("ms.work.unit.partition", String.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) "none";
    }
  },
  MSTAGE_WORK_UNIT_PARTIAL_PARTITION("ms.work.unit.partial.partition", Boolean.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) Boolean.TRUE;
    }
  },
  MSTAGE_WORK_UNIT_PACING_SECONDS("ms.work.unit.pacing.seconds", Integer.class) {
    @Override
    public Long getMillis(State state) {
      return 1000L * (Integer) this.getProp(state);
    }
  },
  // this is an internal property, its purpose is to pass value between Source and Extractor
  MSTAGE_WORKUNIT_STARTTIME_KEY("ms.work.unit.scheduling.starttime", Long.class) {
    @Override
    public <T> T getDefaultValue() {
      return (T) new Long(0L);
    }
  };

  final static private Gson GSON = new Gson();
  final static private String PROPERTY_SEPARATOR = ".";

  private final String config;
  private final Class<?> className;
  private final Object defaultValue;

  MultistageProperties(String config, Class<?> className) {
    this.config = config;
    this.className = className;
    this.defaultValue = null;
  }

  @Override
  public String toString() {
    return config;
  }

  /**
   * validate accepts blank entry and validates the value when it is non-blank
   * <p>
   * This version serves those Source properties
   * <p>
   * @param state source state
   * @return true when the configuration is blank or non-blank and valid
   */
  public boolean validate(State state) {
    return true;
  };

  /**
   * validate rejects blank entry and only validates the value when it is non-blank
   * <p>
   * This version serves those Source properties, the duplication is not ideal, we could
   * make this better by define the getProp methods on State, instead of WorkUnitState and
   * SourceState separately in Gobblin core.
   * <p>
   * @param state source state
   * @return true when the configuration is non-blank and valid
   */
  public boolean validateNonblank(State state) {
    try {
      if (className == JsonArray.class) {
        return ((JsonArray) getProp(state)).size() > 0;
      }

      if (className == JsonObject.class) {
        return ((JsonObject) getProp(state)).entrySet().size() > 0;
      }

      if (className == Boolean.class) {
        // cannot call getPropAsBoolean to tell if a configuration exists
        // as FALSE will be return from empty configuration
        String prop = state.getProp(config, StringUtils.EMPTY);
        return StringUtils.isNotBlank(prop)
            && (prop.equalsIgnoreCase("true") || prop.equalsIgnoreCase("false"));
      }

      if (className == String.class) {
        return StringUtils.isNotEmpty(getProp(state));
      }

      if (className == Integer.class) {
        return (Integer) getProp(state) != 0;
      }

      if (className == Long.class) {
        return (Long) getProp(state) != 0;
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public Long getMillis(State state) {
    return 0L;
  }

  /**
   * get property value for a specified JobProperties item
   * <p>
   * This version serves those Source properties, the duplication is not ideal, we could
   * make this better by define the getProp methods on State, instead of WorkUnitState and
   * SourceState separately in Gobblin core.
   * <p>
   * @param state the source or work unit state
   * @param <T> the template class type
   * @return the property value of the specific JobProperties item
   */
  public <T> T getProp(State state) {
    if (className == Boolean.class) {
      return (T) new Boolean(state.getPropAsBoolean(config));
    } else if (className == Integer.class) {
      return (T) new Integer(state.getPropAsInt(config, 0));
    } else if (className == Long.class) {
      return (T) new Long(state.getPropAsLong(config, 0L));
    } else if (className == String.class) {
      return (T) state.getProp(config, StringUtils.EMPTY);
    } else if (className == JsonArray.class) {
      return (T) GSON.fromJson(state.getProp(config, new JsonArray().toString()), JsonArray.class);
    } else if (className == JsonObject.class) {
      return (T) GSON.fromJson(state.getProp(config, new JsonObject().toString()), JsonObject.class);
    }
    return null;
  }

  public <T> T getValidNonblankWithDefault(State state) {
    if (className == JsonArray.class) {
      return (T) (validateNonblank(state) ? getProp(state) : getDefaultValue());
    }
    if (className == JsonObject.class) {
      return (T) (validateNonblank(state) ? getProp(state) : getDefaultValue());
    }
    if (className == Long.class) {
      return (T) ((validateNonblank(state)) ? getProp(state) : getDefaultValue());
    }
    if (className == String.class) {
      return (T) ((validateNonblank(state)) ? getProp(state) : getDefaultValue());
    }
    if (className == Integer.class) {
      return (T) ((validateNonblank(state)) ? getProp(state) : getDefaultValue());
    }
    if (className == Boolean.class) {
      return (T) ((validateNonblank(state)) ? getProp(state) : getDefaultValue());
    }
    return getProp(state);
  }

  public <T> T getDefaultValue() {
    if (className == JsonArray.class) {
      return (T) new JsonArray();
    }
    if (className == JsonObject.class) {
      return (T) new JsonObject();
    }
    if (className == String.class) {
      return (T) StringUtils.EMPTY;
    }
    if (className == Long.class) {
      return (T) new Long(0L);
    }
    if (className == Integer.class) {
      return (T) new Integer(0);
    }
    return null;
  }
}
// END of enum JobProperties