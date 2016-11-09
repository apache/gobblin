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

package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;

/**
 * Used to indicate the interface for generating the {@link CopyRoute} based on 
 * 
 * <ul>
 *  <li>{@link ReplicationConfiguration}
 *  <li>in Pull mode, the copy to {@link EndPoint}
 *  <li>in Push mode, the copy from {@link EndPoint}
 * </ul>
 * @author mitu
 *
 */
public interface CopyRouteGenerator {
  
  // implied for pull mode
  public Optional<CopyRoute> getPullRoute(ReplicationConfiguration rc, EndPoint copyTo);
  
  // implied for push mode
  public Optional<List<CopyRoute>> getPushRoutes(ReplicationConfiguration rc, EndPoint copyFrom);
  
}
