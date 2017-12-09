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
package gobblin.source.workunit;

import gobblin.configuration.SourceState;
import gobblin.source.extractor.WatermarkInterval;

/***
 * Shim layer for org.apache.gobblin.source.workunit.WorkUnit
 */
public class WorkUnit extends org.apache.gobblin.source.workunit.WorkUnit {
  @Deprecated
  public WorkUnit() {
    super();
  }

  @Deprecated
  public WorkUnit(SourceState state, Extract extract) {
    super(state, extract);
  }

  @Deprecated
  public WorkUnit(SourceState state, Extract extract, WatermarkInterval watermarkInterval) {
    super(state, extract, watermarkInterval);
  }

  public WorkUnit(Extract extract) {
    super(extract);
  }

  @Deprecated
  public WorkUnit(WorkUnit other) {
    super(other);
  }
}
