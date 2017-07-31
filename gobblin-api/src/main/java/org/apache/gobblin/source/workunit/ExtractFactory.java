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

package org.apache.gobblin.source.workunit;

import java.util.Locale;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.apache.gobblin.source.workunit.Extract.TableType;


public class ExtractFactory {
  private final Set<Extract> createdInstances;
  private final DateTimeFormatter dtf;

  public ExtractFactory(String dateTimeFormat) {
    this.createdInstances = Sets.newHashSet();
    this.dtf = DateTimeFormat.forPattern(dateTimeFormat).withLocale(Locale.US).withZone(DateTimeZone.UTC);
  }

  /**
   * Returns a unique {@link Extract} instance.
   * Any two calls of this method from the same {@link ExtractFactory} instance guarantees to
   * return {@link Extract}s with different IDs.
   *
   * @param type {@link TableType}
   * @param namespace dot separated namespace path
   * @param table table name
   * @return a unique {@link Extract} instance
   */
  public synchronized Extract getUniqueExtract(TableType type, String namespace, String table) {
    Extract newExtract = new Extract(type, namespace, table);
    while (this.createdInstances.contains(newExtract)) {
      if (Strings.isNullOrEmpty(newExtract.getExtractId())) {
        newExtract.setExtractId(this.dtf.print(new DateTime()));
      } else {
        DateTime extractDateTime = this.dtf.parseDateTime(newExtract.getExtractId());
        newExtract.setExtractId(this.dtf.print(extractDateTime.plusSeconds(1)));
      }
    }
    this.createdInstances.add(newExtract);
    return newExtract;
  }
}
