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
package gobblin.data.management.conversion.hive.watermarker;

import gobblin.configuration.State;

/**
 * An interface for creating new {@link HiveSourceWatermarker}s
 */
public interface HiveSourceWatermarkerFactory {
  /**
   * Create new {@link HiveSourceWatermarker} from {@link State}
   */
  public HiveSourceWatermarker createFromState(State state);
}
