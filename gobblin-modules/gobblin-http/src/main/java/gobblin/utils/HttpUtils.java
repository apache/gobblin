package gobblin.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import gobblin.http.HttpOperation;
import gobblin.util.AvroUtils;


/**
 * Utilities to build gobblin http components
 */
public class HttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);
  private static final Gson GSON = new Gson();

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
      LOG.error("Fail to build url", e);
      return null;
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
