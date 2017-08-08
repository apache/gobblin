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

package org.apache.gobblin.metrics.broker;

import com.google.common.collect.ImmutableMap;


/**
 * A {@link org.apache.gobblin.broker.iface.SharedResourceKey} for {@link MetricContextFactory}. While {@link MetricContextKey}
 * creates a {@link org.apache.gobblin.metrics.MetricContext} with keys extracted from the broker configuration, this factory creates
 * a {@link org.apache.gobblin.metrics.MetricContext} which is a child of the former context, and which has tags and names specified
 * in the key.
 *
 * This key is useful when a construct needs to acquire a variety of tagged {@link org.apache.gobblin.metrics.MetricContext} with
 * different tags.
 */
public class SubTaggedMetricContextKey extends MetricContextKey {
  private final String metricContextName;
  private final ImmutableMap<String, String> tags;

  @java.beans.ConstructorProperties({"metricContextName", "tags"})
  public SubTaggedMetricContextKey(String metricContextName, ImmutableMap<String, String> tags) {
    this.metricContextName = metricContextName;
    this.tags = tags;
  }

  public String getMetricContextName() {
    return this.metricContextName;
  }

  public ImmutableMap<String, String> getTags() {
    return this.tags;
  }

  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    if (o == this) {
      return true;
    }
    if (!(o instanceof SubTaggedMetricContextKey)) {
      return false;
    }
    final SubTaggedMetricContextKey other = (SubTaggedMetricContextKey) o;
    if (!other.canEqual((Object) this)) {
      return false;
    }
    final Object this$metricContextName = this.getMetricContextName();
    final Object other$metricContextName = other.getMetricContextName();
    if (this$metricContextName == null ? other$metricContextName != null
        : !this$metricContextName.equals(other$metricContextName)) {
      return false;
    }
    final Object this$tags = this.getTags();
    final Object other$tags = other.getTags();
    if (this$tags == null ? other$tags != null : !this$tags.equals(other$tags)) {
      return false;
    }
    return true;
  }

  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final Object $metricContextName = this.getMetricContextName();
    result = result * PRIME + ($metricContextName == null ? 43 : $metricContextName.hashCode());
    final Object $tags = this.getTags();
    result = result * PRIME + ($tags == null ? 43 : $tags.hashCode());
    return result;
  }

  protected boolean canEqual(Object other) {
    return other instanceof SubTaggedMetricContextKey;
  }

  public String toString() {
    return "gobblin.metrics.broker.SubTaggedMetricContextKey(metricContextName=" + this.getMetricContextName()
        + ", tags=" + this.getTags() + ")";
  }
}
