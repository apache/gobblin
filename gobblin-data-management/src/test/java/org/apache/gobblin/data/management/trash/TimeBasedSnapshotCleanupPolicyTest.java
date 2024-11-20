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
import java.util.Properties;

public class TimeBasedSnapshotCleanupPolicyTest {

    private TimeBasedSnapshotCleanupPolicy cleanupPolicy;

    @BeforeMethod
    public void setUp()  {
        // Initialize the cleanup policy with a retention period of 600 minutes (10 hours)
        Properties properties = new Properties();
        properties.setProperty(TimeBasedSnapshotCleanupPolicy.SNAPSHOT_RETENTION_POLICY_MINUTES_KEY, "600"); // 600 minutes = 10 hours
        cleanupPolicy = new TimeBasedSnapshotCleanupPolicy(properties);

    }

    @Test
    public void testShouldDeleteSnapshot() throws IOException {

        // Create a Trash
        TrashTestBase trashTestBase = new TrashTestBase(new Properties());
        Trash trash = trashTestBase.trash;

        // Get the current time
        DateTime now = DateTime.now(DateTimeZone.UTC);

        // Create dummy paths with timestamps between 11 and 9 hours ago in UTC
        FileStatus fs1 = new FileStatus(0, true, 0, 0, 0,
                new Path(trash.getTrashLocation(), now.minusHours(11).toString(Trash.TRASH_SNAPSHOT_NAME_FORMATTER)));
        FileStatus fs2 = new FileStatus(0, true, 0, 0, 0,
                new Path(trash.getTrashLocation(), now.minusHours(9).toString(Trash.TRASH_SNAPSHOT_NAME_FORMATTER)));           

        // Test snapshot (should be deleted)
        Assert.assertTrue(cleanupPolicy.shouldDeleteSnapshot(fs1, trash),"Snapshot should be deleted");
        // Test snapshot (should NOT be deleted)
        Assert.assertFalse(cleanupPolicy.shouldDeleteSnapshot(fs2, trash),"Snapshot should NOT be deleted");
    }
}