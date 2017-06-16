package gobblin.utils;

public class HttpConstants {
  /** Configuration keys */
  public static final String URL_TEMPLATE = "urlTemplate";
  public static final String VERB = "verb";
  public static final String PROTOCOL_VERSION = "protocolVersion";
  public static final String CONTENT_TYPE = "contentType";

  /** HttpOperation avro record field names */
  public static final String KEYS = "keys";
  public static final String QUERY_PARAMS = "queryParams";
  public static final String HEADERS = "headers";
  public static final String BODY = "body";

  /** Status code */
  public static final String ERROR_CODE_WHITELIST = "errorCodeWhitelist";
  public static final String CODE_3XX = "3XX";
  public static final String CODE_4XX = "4XX";
  public static final String CODE_5XX = "5XX";
}
