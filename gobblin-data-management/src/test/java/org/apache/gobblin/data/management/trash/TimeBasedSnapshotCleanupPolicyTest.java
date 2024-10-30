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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.sql.Date;
import java.util.Properties;

public class TimeBasedSnapshotCleanupPolicyTest {

    private MockTimeBasedSnapshotCleanupPolicy cleanupPolicy;

    @BeforeMethod
    public void setUp()  {
        // Initialize the cleanup policy with a retention period (e.g., 1 day)
        Properties properties = new Properties();
        properties.setProperty(MockTimeBasedSnapshotCleanupPolicy.SNAPSHOT_RETENTION_POLICY_MINUTES_KEY, "1440"); // 1440 minutes = 1 day
        properties.setProperty(MockTimeBasedSnapshotCleanupPolicy.USE_UTC_COMPARISON_KEY, "true");
        // Mock the cutoff time to be 2024-10-30 01:01:00 UTC
        cleanupPolicy = new MockTimeBasedSnapshotCleanupPolicy(properties, new DateTime(2024, 10, 30, 1, 1));
    }

    @Test
    public void testShouldDeleteSnapshot() throws IOException {

        // Create a Trash
        TrashTestBase trashTestBase = new TrashTestBase(new Properties());
        Trash trash = trashTestBase.trash;

        // Create dummy paths
        FileStatus fs1 = new FileStatus(0, true, 0, 0, 0, 
                                        new Path(trash.getTrashLocation(), new DateTime(2024, 10, 29, 1, 0, DateTimeZone.UTC).toString(Trash.TRASH_SNAPSHOT_NAME_FORMATTER)));
        FileStatus fs2 = new FileStatus(0, true, 0, 0, 0, 
                                        new Path(trash.getTrashLocation(), new DateTime(2024, 10, 29, 2, 0, DateTimeZone.UTC).toString(Trash.TRASH_SNAPSHOT_NAME_FORMATTER)));

        // Test old snapshot (should be deleted)
        // 2024-10-29 01:00:00 UTC + 1440 minutes = 2024-10-30 01:00:00 UTC < Cutoff time a.k.a system current_time (2024-10-30 01:01:00 UTC)
        Assert.assertTrue(cleanupPolicy.shouldDeleteSnapshot(fs1, trash), "Old snapshot should be deleted");

        // Test snapshot (should not be deleted)
        // 2024-10-29 02:00:00 UTC + 1440 minutes = 2024-10-30 02:00:00 UTC > Cutoff time a.k.a system current_time (2024-10-30 01:01:00 UTC)
        Assert.assertFalse(cleanupPolicy.shouldDeleteSnapshot(fs2, trash), "snapshot should not be deleted");
    }

    /**
     * Mock the TimeBasedSnapshotCleanupPolicy for testing purposes
     * 
     * In this class, the current time used in the comparison method isBefore() can be mocked 
     * Why? The current time is used to determine if a snapshot is older than the retention period, 
     * and given that the current time is always changing, it is difficult to test the method shouldDeleteSnapshot()
     */
    public class MockTimeBasedSnapshotCleanupPolicy implements SnapshotCleanupPolicy {

        public static final String SNAPSHOT_RETENTION_POLICY_MINUTES_KEY = "gobblin.trash.snapshot.retention.minutes";
        public static final int SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT = 1440; // one day
        public static final String USE_UTC_COMPARISON_KEY = "gobblin.trash.snapshot.retention.comparison.useUTC";
        public static final boolean USE_UTC_COMPARISON_DEFAULT = false;

        private final int retentionMinutes;
        private final boolean useUTCComparison;
        private final DateTime MOCK_CURRENT_TIME;

        public MockTimeBasedSnapshotCleanupPolicy(Properties props, DateTime mockCurrentTime) {
            this.retentionMinutes = Integer.parseInt(props.getProperty(SNAPSHOT_RETENTION_POLICY_MINUTES_KEY,
                Integer.toString(SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT)));
            this.useUTCComparison = Boolean.parseBoolean(props.getProperty(USE_UTC_COMPARISON_KEY, 
                Boolean.toString(USE_UTC_COMPARISON_DEFAULT)));
            this.MOCK_CURRENT_TIME = mockCurrentTime;
        }

        @Override
        public boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash) {
            DateTime snapshotTime = Trash.TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(snapshot.getPath().getName());

            if (this.useUTCComparison) {
                // Ensure that the comparison between snapshotTime and the current time is done in the same time zone (UTC)
                return snapshotTime.plusMinutes(this.retentionMinutes).isBefore(this.MOCK_CURRENT_TIME.withZoneRetainFields(DateTimeZone.UTC));
            } else {
                // Default to use system time zone
                return snapshotTime.plusMinutes(this.retentionMinutes).isBefore(this.MOCK_CURRENT_TIME);
            }
        }
    }

}