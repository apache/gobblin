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

package gobblin.commit;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * The semantics for data delivery.
 *
 * @author ziliu
 */
public enum DeliverySemantics {

  /**
   * Each data record from the source is guaranteed to be delivered at least once.
   */
  AT_LEAST_ONCE,

  /**
   * Each data record from the source is guaranteed to be delievered exactly once.
   */
  EXACTLY_ONCE;

  /**
   * Get the devliery semantics type from {@link ConfigurationKeys#DELIVERY_SEMANTICS}.
   * The default value is {@link Type#AT_LEAST_ONCE}.
   */
  public static DeliverySemantics parse(State state) {
    String value =
        state.getProp(ConfigurationKeys.GOBBLIN_RUNTIME_DELIVERY_SEMANTICS, AT_LEAST_ONCE.toString()).toUpperCase();
    Optional<DeliverySemantics> semantics = Enums.getIfPresent(DeliverySemantics.class, value);
    Preconditions.checkState(semantics.isPresent(), value + " is not a valid delivery semantics");
    return semantics.get();
  }
}
