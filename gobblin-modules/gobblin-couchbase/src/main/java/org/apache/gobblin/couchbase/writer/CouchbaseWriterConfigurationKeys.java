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

package org.apache.gobblin.couchbase.writer;

import java.util.Collections;
import java.util.List;


public class CouchbaseWriterConfigurationKeys {

  public static final String COUCHBASE_WRITER_PREFIX="writer.couchbase.";

  private static String prefix(String value) { return COUCHBASE_WRITER_PREFIX + value;};

  public static final String BOOTSTRAP_SERVERS= prefix("bootstrapServers");
  public static final List<String> BOOTSTRAP_SERVERS_DEFAULT= Collections.singletonList("localhost");

  public static final String BUCKET=prefix("bucket");
  public static final String BUCKET_DEFAULT = "default";
  public static final String PASSWORD = prefix("password");

  public static final String OPERATION_TIMEOUT_MILLIS = "operationTimeoutMillis";
  public static final long OPERATION_TIMEOUT_DEFAULT = 10000; // 10 second default timeout

}
