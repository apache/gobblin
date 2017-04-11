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

package gobblin.metrics;

import java.util.Collection;
import java.util.List;


/**
 * An interface for classes with which {@link Tag}s can be associated.
 *
 * @author Yinan Li
 */
public interface Taggable {

  /**
   * Add a single {@link Tag}.
   *
   * <p>
   *   The order in which {@link Tag}s are added is important as this is the order
   *   the tag names appear in the metric name prefix.
   * </p>
   *
   * @param tag the {@link Tag} to add
   */
  public void addTag(Tag<?> tag);

  /**
   * Add a collection of {@link Tag}s.
   *
   * @param tags the collection of {@link Tag}s to add
   */
  public void addTags(Collection<Tag<?>> tags);

  /**
   * Get all {@link Tag}s in a list.
   *
   * <p>
   *   This method guarantees no duplicated {@link Tag}s and the order of {@link Tag}s
   *   is the same as the one in which the {@link Tag}s were added.
   * </p>
   *
   * @return all {@link Tag}s in a list
   */
  public List<Tag<?>> getTags();

  /**
   * Construct a metric name prefix from the {@link Tag}s.
   *
   * <p>
   *   The prefix will include both the key and value of every {@link Tag} in the form of {@code key:value}
   *   if {@code includeTagKeys} is {@code true}, otherwise it only includes the value of every {@link Tag}.
   * </p>
   *
   * @param includeTagKeys whether to include tag keys in the metric name prefix
   * @return a metric name prefix constructed from the {@link Tag}s
   */
  public String metricNamePrefix(boolean includeTagKeys);
}
