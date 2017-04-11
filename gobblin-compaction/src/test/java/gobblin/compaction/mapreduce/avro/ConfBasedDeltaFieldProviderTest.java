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

package gobblin.compaction.mapreduce.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test class for {@link ConfBasedDeltaFieldProvider}.
 */
@Test(groups = {"gobblin.compaction"})
public class ConfBasedDeltaFieldProviderTest {

  @Test
  public void testGetDeltaFieldNamesForNewSchema() {
    Configuration conf = mock(Configuration.class);
    when(conf.get(ConfBasedDeltaFieldProvider.DELTA_FIELDS_KEY)).thenReturn("scn");
    AvroDeltaFieldNameProvider provider = new ConfBasedDeltaFieldProvider(conf);
    GenericRecord fakeRecord = mock(GenericRecord.class);

    List<String> deltaFields = provider.getDeltaFieldNames(fakeRecord);
    Assert.assertEquals(deltaFields.size(), 1);
    Assert.assertEquals(deltaFields.get(0), "scn");

    when(conf.get(ConfBasedDeltaFieldProvider.DELTA_FIELDS_KEY)).thenReturn("scn, scn2");
    AvroDeltaFieldNameProvider provider2 = new ConfBasedDeltaFieldProvider(conf);
    List<String> deltaFields2 = provider2.getDeltaFieldNames(fakeRecord);
    Assert.assertEquals(deltaFields2.size(), 2);
    Assert.assertEquals(deltaFields2.get(0), "scn");
    Assert.assertEquals(deltaFields2.get(1), "scn2");
  }
}
