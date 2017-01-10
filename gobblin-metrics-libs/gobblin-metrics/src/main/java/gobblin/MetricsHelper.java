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

package gobblin;

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;


public class MetricsHelper {

  public static Map<String, Long> dumpMetrics(MetricContext context) {
    Map<String, Long> output = new HashMap<>();
    for (Map.Entry<String, Meter> entry : context.getMeters().entrySet()) {
      output.put(entry.getKey(), entry.getValue().getCount());
    }
    for (Map.Entry<String, Timer> entry : context.getTimers().entrySet()) {
      output.put(entry.getKey(), entry.getValue().getCount());
    }
    return output;
  }

  public static Map<String, String> dumpTags(MetricContext context) {
    Map<String, String> output = new HashMap<>();
    for (Tag<?> tag : context.getTags()) {
      output.put(tag.getKey(), tag.getValue().toString());
    }
    return output;
  }

}
