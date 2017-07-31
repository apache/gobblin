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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public abstract class ProducerJob {
  static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  static final GsonBuilder gsonBuilder = new GsonBuilder();

  public abstract String getPage();

  /**
   * format is "yyyy-MM-dd"
   */
  public abstract String getStartDate();

  /**
   * format is "yyyy-MM-dd"
   */
  public abstract String getEndDate();

  public abstract GoogleWebmasterFilter.FilterOperator getOperator();

  /**
   * return how many pages are included in this job
   */
  public abstract int getPagesSize();

  public List<? extends ProducerJob> partitionJobs() {
    DateTime start = dateFormatter.parseDateTime(getStartDate());
    DateTime end = dateFormatter.parseDateTime(getEndDate());
    int days = Days.daysBetween(start, end).getDays();
    if (days <= 0) {
      return new ArrayList<>();
    }
    int step = days / 2;
    return Arrays.asList(new SimpleProducerJob(getPage(), getStartDate(), dateFormatter.print(start.plusDays(step))),
        new SimpleProducerJob(getPage(), dateFormatter.print(start.plusDays(step + 1)), getEndDate()));
  }

  public static String serialize(Collection<ProducerJob> jobs) {
    //TODO: don't need to recreate objects if it's of type SimpleProducerJob
    Collection<ProducerJob> producerJobs = new ArrayList<>(jobs.size());
    for (ProducerJob job : jobs) {
      producerJobs.add(new SimpleProducerJob(job));
    }
    Gson gson = gsonBuilder.create();
    return gson.toJson(producerJobs);
  }
}
