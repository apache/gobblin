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
package gobblin.converter.http;

import com.google.common.base.Optional;

import lombok.Data;

/**
 * Rest Entry that hold resource path and resource value.
 */
@Data
public class RestEntry<T> {
  private final Optional<String> resourcePath;
  private final T restEntryVal;

  public RestEntry(String resourcePath, T restEntryVal) {
    this.resourcePath = Optional.fromNullable(resourcePath);
    this.restEntryVal = restEntryVal;
  }
}