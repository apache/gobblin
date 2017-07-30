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

package gobblin.util.filters;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import lombok.ToString;


/**
 * Use a regex to filter {@link Path}. If {@link #include} is set to true, include {@link Path} that matches the regex.
 * Otherwise, include {@link Path} that does not match the regex.
 */
@ToString
public class RegexPathFilter implements PathFilter {

  private final Pattern regex;
  private final boolean include;

  public RegexPathFilter(String regex) {
    this(regex, true);
  }

  public RegexPathFilter(String regex, boolean include) {
    this.regex = Pattern.compile(regex);
    this.include = include;
  }

  @Override
  public boolean accept(Path path) {
    boolean matches = this.regex.matcher(path.getName()).matches();
    return include ? matches : !matches;
  }
}
