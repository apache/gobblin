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

package org.apache.gobblin.hive.spec;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.spec.activity.DropPartitionActivity;
import org.apache.gobblin.hive.spec.activity.DropTableActivity;
import org.apache.gobblin.hive.spec.predicate.PartitionNotExistPredicate;
import org.apache.gobblin.hive.spec.predicate.TableNotExistPredicate;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;

@Test(groups = {"gobblin.hive"})
public class SimpleHiveSpecTest {
    private final String dbName = "db";
    private final String tableName = "tbl";
    private final String pathString = "tbl";
    private SimpleHiveSpec simpleHiveSpec;

    @BeforeClass
    public void setup(){
        HiveTable.Builder tableBuilder = new HiveTable.Builder();
        tableBuilder.withDbName(dbName).withTableName(tableName);
        HiveTable hiveTable = tableBuilder.build();

        SimpleHiveSpec.Builder specBuilder = new SimpleHiveSpec.Builder(new Path(pathString))
                .withPartition(Optional.absent())
                .withTable(hiveTable);
        simpleHiveSpec = specBuilder.build();
    }

    @Test(priority=1)
    public void testBuildSimpleSpec() {
        Assert.assertEquals(simpleHiveSpec.getTable().getDbName(), dbName);
        Assert.assertEquals(simpleHiveSpec.getTable().getTableName(), tableName);
        Assert.assertEquals(0, simpleHiveSpec.getPostActivities().size());
        Assert.assertEquals(0, simpleHiveSpec.getPreActivities().size());
        Assert.assertEquals(0, simpleHiveSpec.getPredicates().size());
        Assert.assertEquals(Optional.absent(), simpleHiveSpec.getPartition());

        String actualString = Objects.toStringHelper(simpleHiveSpec).omitNullValues().add("path", pathString)
                .add("db", dbName).add("table", tableName)
                .add("partition", Optional.absent().orNull()).toString();
        Assert.assertEquals(actualString, simpleHiveSpec.toString());
    }

    @Test(priority=2)
    public void testActivity(){
        DropPartitionActivity dropPartitionActivity = new DropPartitionActivity(dbName, tableName,
                new ArrayList<>(), new ArrayList<>());
        DropTableActivity dropTableActivity = new DropTableActivity(dbName, tableName);

        simpleHiveSpec.getPreActivities().add(dropPartitionActivity);
        simpleHiveSpec.getPreActivities().add(dropTableActivity);

        simpleHiveSpec.getPostActivities().add(dropPartitionActivity);

        Assert.assertEquals(simpleHiveSpec.getPreActivities().size(), 2);
        Assert.assertEquals(simpleHiveSpec.getPostActivities().size(), 1);
    }

    @Test(priority=3)
    public void testPredicate(){
        TableNotExistPredicate tableNotExistPredicate = new TableNotExistPredicate(dbName, tableName);
        PartitionNotExistPredicate partitionNotExistPredicate = new PartitionNotExistPredicate(dbName, tableName,
                new ArrayList<>(), new ArrayList<>());

        simpleHiveSpec.getPredicates().add(tableNotExistPredicate);
        simpleHiveSpec.getPredicates().add(partitionNotExistPredicate);

        Assert.assertEquals(simpleHiveSpec.getPredicates().size(), 2);
    }
}
