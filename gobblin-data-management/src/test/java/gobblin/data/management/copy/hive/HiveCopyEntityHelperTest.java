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

package gobblin.data.management.copy.hive;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HiveCopyEntityHelperTest {

  @Test public void testResolvePath() throws Exception {
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data/$DB/$TABLE", "db", "table"), new Path("/data/db/table"));
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data/$TABLE", "db", "table"), new Path("/data/table"));
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data", "db", "table"), new Path("/data/table"));

  }

}
