/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.callback;

import lombok.Getter;

import javax.annotation.Nullable;

import java.util.List;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Function;
import com.google.common.base.Predicate;

import gobblin.metrics.notification.Notification;


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
