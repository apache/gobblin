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

package gobblin.metrics.metric.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;


/**
 * Implementation of {@link MetricFilter} that takes in a regex, and {@link #matches(String, Metric)} a {@link Metric}
 * if the name of the {@link Metric} matches the regex.
 */
public class MetricNameRegexFilter implements MetricFilter {

  private final Pattern regex;
  private final Matcher matcher;

  public MetricNameRegexFilter(String regex) {
    this.regex = Pattern.compile(regex);
    this.matcher = this.regex.matcher("");
  }

  @Override
  public boolean matches(String name, Metric metric) {
    return this.matcher.reset(name).matches();
  }
}
