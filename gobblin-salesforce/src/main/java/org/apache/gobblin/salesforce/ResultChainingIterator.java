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

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.gson.JsonElement;
import com.sforce.async.BulkConnection;

import lombok.extern.slf4j.Slf4j;


/**
 * The Iterator to chain all result iterators together.
 * It is to create only one iterator for a list of result files of BulkAPI.
 * Same time it can also be able to add other iterator with function `add` to combine to 1 iterator
 */
@Slf4j
public class ResultChainingIterator implements Iterator<JsonElement> {
  private Iterator<JsonElement> iter;
  private int recordCount = 0;
  private int isDeletedRecordCount = 0;

  public ResultChainingIterator(BulkConnection conn, List<FileIdVO> fileIdList, int retryLimit,
      long retryInterval, long retryExceedQuotaInterval) {
    Iterator<BulkResultIterator> iterOfFiles = fileIdList.stream().map(x ->
        new BulkResultIterator(conn, x, retryLimit, retryInterval, retryExceedQuotaInterval)).iterator();
    iter = Iterators.<JsonElement>concat(iterOfFiles);
  }

  public Iterator<JsonElement> get() {
    return iter;
  }

  public void add(Iterator<JsonElement> iter) {
    if (iter != null) {
      this.iter = Iterators.concat(this.iter, iter);
    }
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public JsonElement next() {
    JsonElement jsonElement = iter.next();
    recordCount ++;
    JsonElement isDeletedElement = jsonElement.getAsJsonObject().get("IsDeleted");
    if (isDeletedElement != null && isDeletedElement.getAsBoolean()) {
      isDeletedRecordCount ++;
    }
    if (!iter.hasNext()) {
      // `jsonElement` has last record, print out total and isDeleted=true records(soft deleted) total
      log.info("====Total records: [{}] isDeleted=true records: [{}]====", recordCount, isDeletedRecordCount);
    }
    return jsonElement;
  }
}
