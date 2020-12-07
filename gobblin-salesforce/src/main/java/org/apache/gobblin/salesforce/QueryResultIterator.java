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

package org.apache.gobblin.salesforce;

import com.google.gson.JsonElement;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Iterator for rest api query
 * It is a wrapper of
 * RestApiExtractor.getRecordSet(schema, entity, workUnit, predicateList)
 * the reason why we want to a wrapper for the function is -
 * We want to delay the execution of the function. Only when the next get called, we fetch the data.
 */
@Slf4j
public class QueryResultIterator implements Iterator<JsonElement> {

  private int recordCount = 0;
  private SalesforceExtractor extractor;
  private String schema;
  private String entity;
  private WorkUnit workUnit;
  private List<Predicate> predicateList;

  private Iterator<JsonElement> queryResultIter;

  public QueryResultIterator(
      SalesforceExtractor extractor,
      String schema,
      String entity,
      WorkUnit workUnit,
      List<Predicate> predicateList
  ) {
    log.info("create query result iterator.");
    this.extractor = extractor;
    this.schema = schema;
    this.entity = entity;
    this.workUnit = workUnit;
    this.predicateList = predicateList;
  }

  @Override
  public boolean hasNext() {
    if (queryResultIter == null) {
      initQueryResultIter();
    }
    return queryResultIter.hasNext();
  }

  private void initQueryResultIter() {
    try {
      queryResultIter = extractor.getRecordSet(schema, entity, workUnit, predicateList);
    } catch (DataRecordException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public JsonElement next() {
    if (queryResultIter == null) {
      initQueryResultIter();
    }
    JsonElement jsonElement = queryResultIter.next();
    recordCount ++;
    if (!queryResultIter.hasNext()) {
      // variable `jsonElement` has last record. no more data, print out total
      log.info("----Rest API query records total:{}----", recordCount);
    }
    return jsonElement;
  }
}
