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

package org.apache.gobblin.source.extractor.watermark;

import org.apache.gobblin.source.extractor.extract.QueryBasedExtractor;
import java.util.HashMap;


public interface Watermark {
  /**
   * Condition statement with the water mark value using the operator. Example (last_updated_ts >= 2013-01-01 00:00:00
   *
   * @param extractor
   * @param water mark value
   * @param relational operator between water mark column and value
   * @return condition statement
   */
  public String getWatermarkCondition(QueryBasedExtractor<?, ?> extractor, long watermarkValue, String operator);

  /**
   * Get partitions for the given range
   *
   * @param low water mark value
   * @param high water mark value
   * @param partition interval(in hours or days)
   * @param maximum number of partitions
   * @return partitions
   */
  public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, long partitionInterval,
      int maxIntervals);

  /**
   * Get number of seconds or hour or days or simple number that needs to be added for the successive water mark
   * @return delta value in seconds or hour or days or simple number
   */
  public int getDeltaNumForNextWatermark();
}
