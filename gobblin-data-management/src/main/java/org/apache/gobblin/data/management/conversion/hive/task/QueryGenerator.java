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

package org.apache.gobblin.data.management.conversion.hive.task;

import java.util.List;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;


/**
 * An interface for generating queries.
 */
public interface QueryGenerator {

  /**
   * Generates queries to extract/convert/write data
   * @return list of queries
   */
  List<String> generateQueries();

  /**
   * Generates queries for publish data
   * @return QueryBasedHivePublishEntity containing cleanup and publish queries
   * @throws DataConversionException
   */
  QueryBasedHivePublishEntity generatePublishQueries() throws DataConversionException;
}
