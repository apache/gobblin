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

package org.apache.gobblin.data.management.trash;

import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;


/**
 * Policy that deletes snapshots if they are older than {@link #SNAPSHOT_RETENTION_POLICY_MINUTES_KEY} minutes.
 */
public class TimeBasedSnapshotCleanupPolicy implements SnapshotCleanupPolicy {

  public static final String SNAPSHOT_RETENTION_POLICY_MINUTES_KEY = "gobblin.trash.snapshot.retention.minutes";
  public static final int SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT = 1440; // one day

  private final int retentionMinutes;

  public TimeBasedSnapshotCleanupPolicy(Properties props) {
    this.retentionMinutes = Integer.parseInt(props.getProperty(SNAPSHOT_RETENTION_POLICY_MINUTES_KEY,
        Integer.toString(SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT)));
  }

  @Override
  public boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash) {
    DateTime snapshotTime = Trash.TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(snapshot.getPath().getName());
    System.out.println("Parsed time is " + snapshotTime + " and the timezone is " + snapshotTime.getZone());
    System.out.println("Target clean up time is " + snapshotTime.plusMinutes(this.retentionMinutes));
    System.out.println("Current time is  " + new DateTime() + " and the timezone is " + new DateTime().getZone());

    DateTime now = new DateTime().withZone(DateTimeZone.UTC).minusHours(7); // mimic the time in azkaban 
    DateTime targetCleanupTime = snapshotTime.plusMinutes(this.retentionMinutes);
    DateTime delta = targetCleanupTime.minus(now.getMillis());

    Duration duration = new Duration(now, targetCleanupTime);
    duration.toStandardHours();
    duration.toStandardMinutes();
    System.out.println("Time delta is " +  duration.toStandardHours() + " hours and " + duration.toStandardMinutes() + " minutes");


    return snapshotTime.plusMinutes(this.retentionMinutes).isBeforeNow();
  }
}
