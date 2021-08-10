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

package org.apache.gobblin.multistage.preprocessor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.OutputStream;

/**
 * a base class for dynamic OutputStream preprocessor
 */
abstract public class OutputStreamProcessor implements StreamProcessor<OutputStream> {
  protected JsonObject parameters;

  public OutputStreamProcessor(JsonObject params) {
    this.parameters = params;
  }
  abstract public OutputStream process(OutputStream origStream) throws IOException;

  abstract public String convertFileName(String fileName);
}
