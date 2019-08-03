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

package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;


public class CompactionJobConfiguratorTest {

  @Test
  public void testKeyFieldBlacklist() throws IOException {
    State state = new State();
    state.setProp("compaction.job.key.fieldBlacklist", "auditHeader,value.lumos_dropdate,value.__ETL_SCN");
    state.setProp("compaction.job.dedup.key", "ALL");

    CompactionAvroJobConfigurator configurator = new CompactionAvroJobConfigurator(state);
    try (InputStream keyschema = getClass().getClassLoader().getResourceAsStream("dedup-schema/key-schema.avsc")) {
      Schema topicSchema = new Schema.Parser().parse(keyschema);
      Schema actualKeySchema = configurator.getDedupKeySchema(topicSchema);
      Assert.assertEquals(actualKeySchema.getFields().size(), 2);
      Assert.assertEquals(actualKeySchema.getField("value").schema().getFields().size(), 57);
      Assert.assertFalse(AvroUtils.getFieldSchema(actualKeySchema, "auditheader").isPresent());
      Assert.assertFalse(AvroUtils.getFieldSchema(actualKeySchema, "value.lumos_dropdate").isPresent());
      Assert.assertFalse(AvroUtils.getFieldSchema(actualKeySchema, "value.__ETL_SCN").isPresent());

    }
  }
}
