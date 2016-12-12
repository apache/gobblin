/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.writer;

import java.util.Collections;
import java.util.List;


public class CouchbaseWriterConfigurationKeys {

  public static final String COUCHBASE_WRITER_PREFIX="writer.couchbase.";
  private static String prefix(String value) { return COUCHBASE_WRITER_PREFIX + value;};

  public static final String BOOTSTRAP_SERVERS= prefix("bootstrapServers");
  public static final List<String> BOOTSTRAP_SERVERS_DEFAULT= Collections.singletonList("localhost");

  public static final String BUCKET=prefix("bucket");
  public static final String PASSWORD = prefix("password");

}
