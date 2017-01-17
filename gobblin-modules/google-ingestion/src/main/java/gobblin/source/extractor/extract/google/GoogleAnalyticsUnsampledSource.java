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
package gobblin.source.extractor.extract.google;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;

/**
 * Source for Google analytics unsampled report.
 *
 * For unsampled report, the minimum unit of querying range is date. To increase # of partition
 * use hour on "source.querybased.watermark.type" and have "source.querybased.partition.interval" as 23
 * This will make each partition to have only one day in each query.
 * (Note that, by design, # of max partition is limited by "source.max.number.of.partitions").
 *
 * @param <S>
 * @param <D>
 */
public class GoogleAnalyticsUnsampledSource<S, D> extends QueryBasedSource<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAnalyticsUnsampledSource.class);

  protected static final String GOOGLE_ANALYTICS_SOURCE_PREFIX = "source.google_analytics.";
  protected static final String GA_REPORT_PREFIX = GOOGLE_ANALYTICS_SOURCE_PREFIX + "report.";

  protected static final String METRICS = GA_REPORT_PREFIX + "metrics";
  protected static final String DIMENSIONS = GA_REPORT_PREFIX + "dimensions";
  protected static final String SEGMENTS = GA_REPORT_PREFIX + "segments";
  protected static final String FILTERS = GA_REPORT_PREFIX + "filters";
  protected static final String ACCOUNT_ID = GA_REPORT_PREFIX + "account_id";
  protected static final String WEB_PROPERTY_ID = GA_REPORT_PREFIX + "web_property_id";
  protected static final String VIEW_ID = GA_REPORT_PREFIX + "view_id";
  protected static final String DATE_FORMAT = "yyyy-MM-dd";

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    return new GoogleAnalyticsUnsampledExtractor<>(state);
  }
}
