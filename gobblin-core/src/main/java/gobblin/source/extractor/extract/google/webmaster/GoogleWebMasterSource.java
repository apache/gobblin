package gobblin.source.extractor.extract.google.webmaster;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Google Webmaster API enables you to download data from Google Search Console for search analytics of the verified sites. See more here https://developers.google.com/webmaster-tools/. Configure the Google Webmaster Source for starting a daily job to download search analytics data. This gobblin job partitions the whole task into sub-tasks for each day. Each sub-task is handled by a GoogleWebmasterExtractor for that date, and each GoogleWebmasterExtractor holds a queue of GoogleWebmasterExtractorIterators, each of which does the query task for each filter(Currently, only the country filter is supported.) on that date.
 *
 * The minimum unit of querying range is date. Change the range by configuring "source.querybased.start.value" and "source.querybased.end.value". Note that the analytics data for Google Search Console has a delay or 3 days. So cap your configuration of "source.querybased.append.max.watermark.limit" by "CURRENTDATE-3". See the documentation details of each configuration in the GoogleWebMasterSource fields.
 *
 */
abstract class GoogleWebMasterSource extends QueryBasedSource<String, String[]> {

  /**
   * Must Provide.
   * Provide the location for your Google Service Account private key. This can be generated in your Google API Manager.
   */
  public static final String KEY_CREDENTIAL_LOCATION = "source.google_webmasters.credential.location";
  /**
   * Must Provide.
   * Provide the property site URL whose google search analytics data you want to download
   */
  public static final String KEY_PROPERTY = "source.google_webmasters.property";
  /**
   * Optional: Default to WebmastersScopes.WEBMASTERS_READONLY(which is https://www.googleapis.com/auth/webmasters.readonly)
   *
   * Give a Google API service scope.
   * For Webmaster, only two scopes are supported. WebmastersScopes.WEBMASTERS_READONLY and WebmastersScopes.WEBMASTERS
   */
  public static final String KEY_API_SCOPE = "source.google.api.scope";
  /**
   * The filters that will be passed to all your API requests.
   * Filter format is [GoogleWebmasterFilter.Dimension].[DimensionValue]
   * Currently, this filter operator is "EQUALS" and only Country dimension is supported. Will extend this feature according to more use cases in the futher.
   */
  public static final String KEY_REQUEST_FILTERS = "source.google_webmasters.request.filters";
  /**
   * Must Provide.
   *
   * Allowed dimensions can be found in the enum GoogleWebmasterFilter.Dimension
   */
  public static final String KEY_REQUEST_DIMENSIONS = "source.google_webmasters.request.dimensions";
  /**
   * Must Provide.
   *
   * Allowed metrics can be found in the enum GoogleWebmasterDataFetcher.Metric
   */
  public static final String KEY_REQUEST_METRICS = "source.google_webmasters.request.metrics";
  /**
   * Optional: Default to 5000, which is the maximum allowed.
   *
   * The response row limit when you ask for pages. Set it to 5000 when you want to get all pages, which might be larger than 5000.
   */
  public static final String KEY_REQUEST_PAGE_LIMIT = "source.google_webmasters.request.page_limit";
  /**
   * Optional: Default to String.empty
   * Hot start this service with pre-set pages. Once this is set, the service will ignore KEY_REQUEST_PAGE_LIMIT, and won't get all pages, but use the pre-set pages instead.
   */
  public static final String KEY_REQUEST_HOT_START = "source.google_webmasters.request.hot_start";
  /**
   * Optional: Default to 5000, which is the maximum allowed.
   *
   * The response row limit when you ask for queries.
   */
  public static final String KEY_REQUEST_QUERY_LIMIT = "source.google_webmasters.request.query_limit";

  /**
   * Tune the maximum rounds of retries allowed when API calls failed because of exceeding quota.
   */
  public static final String KEY_REQUEST_TUNING_RETRIES =
      "source.google_webmasters.request.performance_tuning.max_retry_rounds";
  /**
   * Tune the initial cool down time before starting another round of retry.
   */
  public static final String KEY_REQUEST_TUNING_INITIAL_COOL_DOWN =
      "source.google_webmasters.request.performance_tuning.initial_cool_down";
  /**
   * Tune the extra cool down sleep time for each round before starting another round of retry.
   * The total cool down time will be calculated as "initial_cool_down + cool_down_step * round"
   */
  public static final String KEY_REQUEST_TUNING_COOL_DOWN_STEP =
      "source.google_webmasters.request.performance_tuning.cool_down_step";
  /**
   * Tune the speed of API requests.
   */
  public static final String KEY_REQUEST_TUNING_REQUESTS_PER_SECOND =
      "source.google_webmasters.request.performance_tuning.requests_per_second";

  /**
   * Tune the size of a batch. Batch API calls together to reduce the number of HTTP connections.
   * Note: A set of n requests batched together counts toward your usage limit as n requests, not as one request. The batch request is taken apart into a set of requests before processing.
   * Read more at https://developers.google.com/webmaster-tools/v3/how-tos/batch
   */
  public static final String KEY_REQUEST_TUNING_BATCH_SIZE =
      "source.google_webmasters.request.performance_tuning.batch_size";

  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();

  @Override
  public Extractor<String, String[]> getExtractor(WorkUnitState state) throws IOException {
    List<GoogleWebmasterFilter.Dimension> requestedDimensions = getRequestedDimensions(state);
    List<GoogleWebmasterDataFetcher.Metric> requestedMetrics = getRequestedMetrics(state);

    String schema = state.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);
    JsonArray schemaJson = new JsonParser().parse(schema).getAsJsonArray();
    Map<String, Integer> columnPositionMap = new HashMap<>();
    for (int i = 0; i < schemaJson.size(); ++i) {
      JsonElement jsonElement = schemaJson.get(i);
      String columnName = jsonElement.getAsJsonObject().get("columnName").getAsString().toUpperCase();
      columnPositionMap.put(columnName, i);
    }

    validateFilters(state.getProp(GoogleWebMasterSource.KEY_REQUEST_FILTERS));
    validateRequests(columnPositionMap, requestedDimensions, requestedMetrics);
    return createExtractor(state, columnPositionMap, requestedDimensions, requestedMetrics);
  }

  abstract GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException;

  private void validateFilters(String filters) {
    String countryPrefix = "COUNTRY.";

    for (String filter : splitter.split(filters)) {
      if (filter.toUpperCase().startsWith(countryPrefix)) {
        GoogleWebmasterFilter.validateCountryCode(filter.substring(countryPrefix.length()));
      }
    }
  }

  private void validateRequests(Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) {
    for (GoogleWebmasterFilter.Dimension dimension : requestedDimensions) {
      Preconditions.checkState(columnPositionMap.containsKey(dimension.toString()),
          "Your requested dimension must exist in the source.schema.");
    }
    for (GoogleWebmasterDataFetcher.Metric metric : requestedMetrics) {
      Preconditions.checkState(columnPositionMap.containsKey(metric.toString()),
          "Your requested metric must exist in the source.schema.");
    }
  }

  private List<GoogleWebmasterFilter.Dimension> getRequestedDimensions(WorkUnitState wuState) {
    List<GoogleWebmasterFilter.Dimension> dimensions = new ArrayList<>();
    String dimensionsString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_DIMENSIONS);
    for (String dim : splitter.split(dimensionsString)) {
      dimensions.add(GoogleWebmasterFilter.Dimension.valueOf(dim.toUpperCase()));
    }
    return dimensions;
  }

  private List<GoogleWebmasterDataFetcher.Metric> getRequestedMetrics(WorkUnitState wuState) {
    List<GoogleWebmasterDataFetcher.Metric> metrics = new ArrayList<>();
    String metricsString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_METRICS);
    for (String metric : splitter.split(metricsString)) {
      metrics.add(GoogleWebmasterDataFetcher.Metric.valueOf(metric.toUpperCase()));
    }
    return metrics;
  }
}
