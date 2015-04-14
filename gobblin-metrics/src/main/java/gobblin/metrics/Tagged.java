/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;


/**
 * A base implementation of {@link Taggable}.
 *
 * @author ynli
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
    Preconditions.checkNotNull(tag);
    Preconditions.checkNotNull(tag.getValue());
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
   * Get a metric name constructed from the {@link Tag}s.
   */
  @Override
  public String metricNamePrefix(boolean includeTagKeys) {
    return Joiner.on('.').join(includeTagKeys ? getTags() : this.tags.values());
  }
}
