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

package org.apache.gobblin.metrics.callback;

import lombok.Getter;

import javax.annotation.Nullable;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.gobblin.metrics.notification.Notification;


/**
 * Just stores notifications that satisfy a filter for testing.
 */
@Getter
public class NotificationStore implements Function<Notification, Void> {

  private final Predicate<Notification> predicate;
  private final List<Notification> notificationList = Lists.newArrayList();

  public NotificationStore(Predicate<Notification> filter) {
    this.predicate = filter;
  }

  @Nullable @Override public Void apply(Notification input) {
    if(this.predicate.apply(input)) {
      this.notificationList.add(input);
    }
    return null;
  }
}
