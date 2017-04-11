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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * A base implementation of {@link Taggable}.
 *
 * @author Yinan Li
 */
public class Tagged implements Taggable {

  protected final Map<String, Object> tags;

  public Tagged() {
    this.tags = Maps.newLinkedHashMap();
  }

  public Tagged(Collection<Tag<?>> tags) {
    this.tags = Maps.newLinkedHashMap();
    addTags(tags);
  }

  @Override
  public void addTag(Tag<?> tag) {
    Preconditions.checkNotNull(tag, "Cannot add a null Tag");
    Preconditions.checkNotNull(tag.getValue(), "Cannot add a Tag with a null value. Tag: " + tag);
    this.tags.put(tag.getKey(), tag.getValue());
  }

  @Override
  public void addTags(Collection<Tag<?>> tags) {
    for (Tag<?> tag : tags) {
      addTag(tag);
    }
  }

  @Override
  public List<Tag<?>> getTags() {
    ImmutableList.Builder<Tag<?>> builder = ImmutableList.builder();
    for (Map.Entry<String, Object> entry : this.tags.entrySet()) {
      builder.add(new Tag<Object>(entry.getKey(), entry.getValue()));
    }
    return builder.build();
  }

  /**
   * Return tags as a Map.
   * @return map of tags.
   */
  public Map<String, Object> getTagMap() {
    return ImmutableMap.copyOf(this.tags);
  }

  /**
   * Get a metric name constructed from the {@link Tag}s.
   */
  @Override
  public String metricNamePrefix(boolean includeTagKeys) {
    return Joiner.on('.').join(includeTagKeys ? getTags() : this.tags.values());
  }
}
