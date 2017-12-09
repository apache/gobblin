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

package org.apache.gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;


public class SimpleProducerJob extends ProducerJob {
  private final String _page;
  private final String _startDate;
  private final String _endDate;
  private static final GoogleWebmasterFilter.FilterOperator _operator = GoogleWebmasterFilter.FilterOperator.EQUALS;

  SimpleProducerJob(String page, String startDate, String endDate) {
    _page = page;
    _startDate = startDate;
    _endDate = endDate;
  }

  public SimpleProducerJob(ProducerJob job) {
    this(job.getPage(), job.getStartDate(), job.getEndDate());
  }

  public static List<ProducerJob> deserialize(String jobs) {
    if (jobs == null || jobs.trim().isEmpty()) {
      jobs = "[]";
    }
    JsonArray jobsJson = new JsonParser().parse(jobs).getAsJsonArray();
    return new Gson().fromJson(jobsJson, new TypeToken<ArrayList<SimpleProducerJob>>() {
    }.getType());
  }

  @Override
  public String getPage() {
    return _page;
  }

  @Override
  public String getStartDate() {
    return _startDate;
  }

  @Override
  public String getEndDate() {
    return _endDate;
  }

  @Override
  public GoogleWebmasterFilter.FilterOperator getOperator() {
    return _operator;
  }

  @Override
  public int getPagesSize() {
    return 1;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_page, _startDate, _endDate, _operator);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!SimpleProducerJob.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    SimpleProducerJob other = (SimpleProducerJob) obj;
    return Objects.equals(_page, other._page) && Objects.equals(_startDate, other._startDate) && Objects.equals(
        _endDate, other._endDate) && Objects.equals(_operator, other._operator);
  }

  @Override
  public String toString() {
    return String.format("SimpleProducerJob{_page='%s', _startDate='%s', _endDate='%s', _operator=%s}", _page,
        _startDate, _endDate, _operator);
  }
}
