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

package org.apache.gobblin.compaction.mapreduce.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrcKeyComparatorTest {
  @Test
  public void testSimpleComparator() throws Exception {
    OrcKeyComparator comparator = new OrcKeyComparator();
    Configuration conf = new Configuration();
    String orcSchema = "struct<i:int,j:int>";
    TypeDescription schema = TypeDescription.fromString(orcSchema);
    conf.set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), orcSchema);
    Assert.assertEquals(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()), orcSchema);
    comparator.setConf(conf);

    OrcStruct record0 = (OrcStruct) OrcStruct.createValue(schema);
    record0.setFieldValue("i", new IntWritable(1));
    record0.setFieldValue("j", new IntWritable(2));

    OrcStruct record1 = (OrcStruct) OrcStruct.createValue(schema);
    record1.setFieldValue("i", new IntWritable(3));
    record1.setFieldValue("j", new IntWritable(4));

    OrcStruct record2 = (OrcStruct) OrcStruct.createValue(schema);
    record2.setFieldValue("i", new IntWritable(3));
    record2.setFieldValue("j", new IntWritable(4));

    OrcKey orcKey0 = new OrcKey();
    orcKey0.key = record0;
    OrcKey orcKey1 = new OrcKey();
    orcKey1.key = record1;
    OrcKey orcKey2 = new OrcKey();
    orcKey2.key = record2;

    Assert.assertTrue(comparator.compare(orcKey0, orcKey1) < 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey2) == 0);
    Assert.assertTrue(comparator.compare(orcKey1, orcKey0) > 0 );
  }

  @Test
  public void testRecordAsKey() throws Exception {

  }
}
