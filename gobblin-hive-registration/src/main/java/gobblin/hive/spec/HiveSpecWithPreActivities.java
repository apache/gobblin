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

package gobblin.hive.spec;

import java.util.Collection;

import gobblin.annotation.Alpha;
import gobblin.hive.spec.activity.Activity;


/**
 * A {@link HiveSpec} with a set of activities that should be executed prior to the Hive registration.
 */
@Alpha
public interface HiveSpecWithPreActivities extends HiveSpec {

  /**
   * A {@link Collection} of {@link Activity}s that should be executed prior to the Hive registration.
   */
  public Collection<Activity> getPreActivities();
}
