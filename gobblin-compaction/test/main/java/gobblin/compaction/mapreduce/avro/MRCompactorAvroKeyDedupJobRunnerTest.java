/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.mapreduce.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;


@Test(groups = { "gobblin.compaction" })
public class MRCompactorAvroKeyDedupJobRunnerTest {

  private MRCompactorAvroKeyDedupJobRunner runner;
  private Job job;

  @BeforeClass
  public void setUp() throws IOException {
    State state = new State();
    state.setProp(ConfigurationKeys.COMPACTION_TOPIC, "TestTopic");
    state.setProp(ConfigurationKeys.COMPACTION_JOB_INPUT_DIR, "TestInputDir");
    state.setProp(ConfigurationKeys.COMPACTION_JOB_DEST_DIR, "TestDestDir");
    state.setProp(ConfigurationKeys.COMPACTION_JOB_TMP_DIR, "TestTmpDir");
    state.setProp(ConfigurationKeys.COMPACTION_DEDUP_KEY, "key");
    this.runner = new MRCompactorAvroKeyDedupJobRunner(state, FileSystem.get(new Configuration()));
    this.job = Job.getInstance();
  }

  @Test
  public void testGetKeySchemaWithPrimaryKey() throws IOException {
    Schema topicSchema =
        new Schema.Parser().parse(new File("gobblin-test/resource/dedup-schema/dedup-schema-with-pkey.avsc"));
    Schema actualKeySchema = this.runner.getKeySchema(this.job, topicSchema);
    Schema expectedKeySchema =
        new Schema.Parser().parse(new File("gobblin-test/resource/dedup-schema/dedup-schema.avsc"));
    Assert.assertEquals(actualKeySchema, expectedKeySchema);
  }

  @Test
  public void testGetKeySchemaWithoutPrimaryKey() throws IOException {
    Schema topicSchema =
        new Schema.Parser().parse(new File("gobblin-test/resource/dedup-schema/dedup-schema-without-pkey.avsc"));
    Schema actualKeySchema = this.runner.getKeySchema(this.job, topicSchema);
    Assert.assertEquals(actualKeySchema, AvroUtils.removeUncomparableFields(topicSchema).get());
  }
}
