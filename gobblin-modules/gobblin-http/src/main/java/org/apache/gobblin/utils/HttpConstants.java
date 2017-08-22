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
package org.apache.gobblin.utils;

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

  public static final String SCHEMA_D2 = "d2://";

  /** Status code */
  public static final String ERROR_CODE_WHITELIST = "errorCodeWhitelist";
  public static final String CODE_3XX = "3xx";
  public static final String CODE_4XX = "4xx";
  public static final String CODE_5XX = "5xx";

  /** Event constants */
  public static final String REQUEST = "request";
  public static final String STATUS_CODE = "statusCode";
}
