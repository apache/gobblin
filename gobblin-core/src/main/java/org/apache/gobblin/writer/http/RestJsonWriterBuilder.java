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
package org.apache.gobblin.writer.http;

import java.io.IOException;

import com.google.gson.JsonObject;

import org.apache.gobblin.converter.http.RestEntry;
import org.apache.gobblin.writer.DataWriter;

/**
 * Builder that builds RestJsonWriter
 */
public class RestJsonWriterBuilder extends AbstractHttpWriterBuilder<Void, RestEntry<JsonObject>, RestJsonWriterBuilder> {

  @Override
  public DataWriter<RestEntry<JsonObject>> build() throws IOException {
    validate();
    return new RestJsonWriter(this);
  }
}
