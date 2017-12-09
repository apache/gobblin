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
package org.apache.gobblin.writer;

import com.google.common.base.Preconditions;


/**
 * A Factory for handing out WatermarkTracker instances
 */
public class WatermarkTrackerFactory {

  public static class TrackerBehavior {
    boolean trackAll = true;
    boolean trackLast = false;
    boolean ignoreUnacknowledged = false;
    boolean validated = false;

    private TrackerBehavior() {};

    public static TrackerBehavior defaultBehavior() {
      return new TrackerBehavior();
    }

    TrackerBehavior trackAll() {
      trackAll = true;
      trackLast = false;
      return this;
    }

    TrackerBehavior trackLast() {
      trackLast = true;
      trackAll = false;
      return this;
    }

    TrackerBehavior ignoreUnacked() {
      ignoreUnacknowledged = true;
      return this;
    }


    private void validate() {
      Preconditions.checkState(this.trackAll || this.trackLast, "Either trackAll or trackLast must be set");
      this.validated = true;
    }

    TrackerBehavior build() {
      validate();
      return this;
    }

  }


  public static WatermarkTracker getInstance(TrackerBehavior trackerBehavior) {
    Preconditions.checkNotNull(trackerBehavior);
    // Check requirements are consistent
    if (!trackerBehavior.validated) {
      trackerBehavior.validate();
    }

    if (trackerBehavior.trackLast) {
      return new LastWatermarkTracker(trackerBehavior.ignoreUnacknowledged);
    }

    if (trackerBehavior.trackAll) {
      return new MultiWriterWatermarkTracker();
    }

    throw new AssertionError("Could not find an applicable WatermarkTracker for TrackerBehavior : "
        + trackerBehavior.toString());
  }

}
