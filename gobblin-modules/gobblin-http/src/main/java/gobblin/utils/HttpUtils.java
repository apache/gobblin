package gobblin.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.typesafe.config.Config;

import gobblin.http.HttpOperation;
import gobblin.http.ResponseStatus;
import gobblin.http.StatusType;
import gobblin.util.AvroUtils;


/**
 * Utilities to build gobblin http components
 */
public class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);
  private static final Gson GSON = new Gson();
  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();


  /**
   * Convert the given {@link GenericRecord} to {@link HttpOperation}
   */
  public static HttpOperation toHttpOperation(GenericRecord record) {
    if (record instanceof HttpOperation) {
      return (HttpOperation) record;
    }

    HttpOperation.Builder builder = HttpOperation.newBuilder();

    Map<String, String> stringMap = AvroUtils.toStringMap(record.get(HttpConstants.KEYS));
    if (stringMap != null) {
      builder.setKeys(stringMap);
    }
    stringMap = AvroUtils.toStringMap(record.get(HttpConstants.QUERY_PARAMS));
    if (stringMap != null) {
      builder.setQueryParams(stringMap);
    }
    stringMap = AvroUtils.toStringMap(record.get(HttpConstants.HEADERS));
    if (stringMap != null) {
      builder.setHeaders(stringMap);
    }
    Object body = record.get(HttpConstants.BODY);
    if (body != null) {
      builder.setBody(body.toString());
    }

    return builder.build();
  }

  /**
   * Given a url template, interpolate with keys and build the URI after adding query parameters
   *
   * <p>
   *   With url template: http://test.com/resource/(urn:${resourceId})/entities/(entity:${entityId}),
   *   keys: { "resourceId": 123, "entityId": 456 }, queryParams: { "locale": "en_US" }, the outpuT URI is:
   *   http://test.com/resource/(urn:123)/entities/(entity:456)?locale=en_US
   * </p>
   *
   * @param urlTemplate url template
   * @param keys data map to interpolate url template
   * @param queryParams query parameters added to the url
   * @return a uri
   */
  public static URI buildURI(String urlTemplate, Map<String, String> keys, Map<String, String> queryParams) {
    // Compute base url
    String url = urlTemplate;
    if (keys != null && keys.size() != 0) {
      url = StrSubstitutor.replace(urlTemplate, keys);
    }

    try {
      URIBuilder uriBuilder = new URIBuilder(url);
      // Append query parameters
      if (queryParams != null && queryParams.size() != 0) {
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
          uriBuilder.addParameter(entry.getKey(), entry.getValue());
        }
      }
      return uriBuilder.build();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Fail to build uri", e);
    }
  }

  /**
   * Get a {@link List<String>} from a comma separated string
   */
  public static List<String> getStringList(String list) {
    return LIST_SPLITTER.splitToList(list);
  }

  /**
   * Get the error code whitelist from a config
   */
  public static Set<String> getErrorCodeWhitelist(Config config) {
    String list = config.getString(HttpConstants.ERROR_CODE_WHITELIST).toLowerCase();
    return new HashSet<>(getStringList(list));
  }

  /**
   * Update {@link StatusType} of a {@link ResponseStatus} based on statusCode and error code white list
   *
   * @param status a status report after handling the a response
   * @param statusCode a status code in http domain
   * @param errorCodeWhitelist a whitelist specifies what http error codes are tolerable
   */
  public static void updateStatusType(ResponseStatus status, int statusCode, Set<String> errorCodeWhitelist) {
    if (statusCode >= 300 & statusCode < 500) {
      List<String> whitelist = new ArrayList<>();
      whitelist.add(Integer.toString(statusCode));
      if (statusCode > 400) {
        whitelist.add(HttpConstants.CODE_4XX);
      } else {
        whitelist.add(HttpConstants.CODE_3XX);
      }
      if (whitelist.stream().anyMatch(errorCodeWhitelist::contains)) {
        status.setType(StatusType.CONTINUE);
      } else {
        status.setType(StatusType.CLIENT_ERROR);
      }
    } else if (statusCode >= 500) {
      List<String> whitelist = Arrays.asList(Integer.toString(statusCode), HttpConstants.CODE_5XX);
      if (whitelist.stream().anyMatch(errorCodeWhitelist::contains)) {
        status.setType(StatusType.CONTINUE);
      } else {
        status.setType(StatusType.SERVER_ERROR);
      }
    }
  }

  /**
   * Convert a json encoded string to a Map
   *
   * @param jsonString json string
   * @return the Map encoded in the string
   */
  public static Map<String, Object> toMap(String jsonString) {
    Map<String, Object> map = new HashMap<>();
    return GSON.fromJson(jsonString, map.getClass());
  }
}
