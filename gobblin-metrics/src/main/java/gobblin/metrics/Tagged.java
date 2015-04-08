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

import com.codahale.metrics.MetricRegistry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * A base implementation of {@link Taggable}.
 *
 * @author ynli
 */
public class Tagged implements Taggable {

  protected final List<Tag> tags;

  public Tagged() {
    this.tags = Lists.newArrayList();
  }

  public Tagged(List<Tag> tags) {
    this.tags = Lists.newArrayList(tags);
  }

  @Override
  public void addTag(Tag tag) {
    Preconditions.checkNotNull(tag);
    this.tags.add(tag);
  }

  @Override
  public void addTags(Collection<Tag> tags) {
    this.tags.addAll(tags);
  }

  @Override
  public List<Tag> getTags() {
    return ImmutableList.<Tag>builder().addAll(this.tags).build();
  }

  /**
   * Get a metric name constructed from the {@link Tag}s.
   */
  public String metricNamePrefix() {
    if (this.tags.isEmpty()) {
      return "";
    }
    String[] tagNames = new String[this.tags.size() - 1];
    for (int i = 1; i < this.tags.size(); i++) {
      tagNames[i - 1] = this.tags.get(i).getValue().toString();
    }
    return MetricRegistry.name(this.tags.get(0).getValue().toString(), tagNames);
  }
}
