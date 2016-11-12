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


public class GoogleWebMasterSource extends QueryBasedSource<String, String[]> {

  /**
   * Provide the location for your Google Service Account private key. This can be generated in your Google API Manager.
   */
  public static final String KEY_CREDENTIAL_LOCATION = "source.google_webmasters.credential.location";
  /**
   * Provide the property site URL whose google search analytics data you want to download
   */
  public static final String KEY_PROPERTY = "source.google_webmasters.property";
  /**
   * This provides a list of pages that the GoogleWebmasterClient must check.
   * This list keeps the pages that we are most interested in and cannot be missed by GoogleWebmasterClient when calling the getAllPages method.
   * Other pages returned by getAllPages are like icing on the cake.
   */
  public static final String KEY_REQUEST_PAGE_CHECK_LIST = "source.google_webmasters.request.page_check_list";
  /**
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
   * Allowed dimensions can be found in the enum GoogleWebmasterFilter.Dimension
   */
  public static final String KEY_REQUEST_DIMENSIONS = "source.google_webmasters.request.dimensions";
  /**
   * Allowed metrics can be found in the enum GoogleWebmasterClient.Metric
   */
  public static final String KEY_REQUEST_METRICS = "source.google_webmasters.request.metrics";
  /**
   * The response row limit when you ask for pages.
   */
  public static final String KEY_REQUEST_PAGE_LIMIT = "source.google_webmasters.request.page_limit";
  /**
   * The response row limit when you ask for queries.
   */
  public static final String KEY_REQUEST_QUERY_LIMIT = "source.google_webmasters.request.query_limit";
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();

  @Override
  public Extractor<String, String[]> getExtractor(WorkUnitState state) throws IOException {
    List<GoogleWebmasterFilter.Dimension> requestedDimensions = getRequestedDimensions(state);
    List<GoogleWebmasterClient.Metric> requestedMetrics = getRequestedMetrics(state);

    String schema = state.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);
    JsonArray schemaJson = new JsonParser().parse(schema).getAsJsonArray();
    Map<String, Integer> columnPositionMap = new HashMap<>();
    for (int i = 0; i < schemaJson.size(); ++i) {
      JsonElement jsonElement = schemaJson.get(i);
      String columnName = jsonElement.getAsJsonObject().get("columnName").getAsString().toUpperCase();
      columnPositionMap.put(columnName, i);
    }
    ValidateRequests(columnPositionMap, requestedDimensions, requestedMetrics);

    return new GoogleWebmasterExtractor(state, columnPositionMap, requestedDimensions, requestedMetrics);
  }

  private void ValidateRequests(Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<GoogleWebmasterClient.Metric> requestedMetrics) {
    for (GoogleWebmasterFilter.Dimension dimension : requestedDimensions) {
      Preconditions.checkState(columnPositionMap.containsKey(dimension.toString()),
          "Your requested dimension must exist in the source.schema.");
    }
    for (GoogleWebmasterClient.Metric metric : requestedMetrics) {
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

  private List<GoogleWebmasterClient.Metric> getRequestedMetrics(WorkUnitState wuState) {
    List<GoogleWebmasterClient.Metric> metrics = new ArrayList<>();
    String metricsString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_METRICS);
    for (String metric : splitter.split(metricsString)) {
      metrics.add(GoogleWebmasterClient.Metric.valueOf(metric.toUpperCase()));
    }
    return metrics;
  }
}
