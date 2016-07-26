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

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.mapreduce.MRCompactor;
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
    state.setProp(ConfigurationKeys.JOB_NAME_KEY, "MRCompactorAvroKeyDedupJobRunnerTest");
    state.setProp(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, "true");
    Dataset.Builder datasetBuilder = (new Dataset.Builder()).withInputPath(new Path("/tmp"));
    Dataset dataset = datasetBuilder.build();
    dataset.setJobProps(state);
    this.runner = new MRCompactorAvroKeyDedupJobRunner(dataset, FileSystem.get(new Configuration()));
    this.job = Job.getInstance();
  }

  @Test
  public void testGetKeySchemaWithPrimaryKey() throws IOException {
    try (InputStream schemaWithPKey = getClass().getClassLoader().getResourceAsStream("dedup-schema/dedup-schema-with-pkey.avsc");
        InputStream dedupKeySchema = getClass().getClassLoader().getResourceAsStream("dedup-schema/dedup-schema.avsc")) {
      Schema topicSchema = new Schema.Parser().parse(schemaWithPKey);
      Schema actualKeySchema = this.runner.getKeySchema(this.job, topicSchema);
      Schema expectedKeySchema = new Schema.Parser().parse(dedupKeySchema);
      Assert.assertEquals(actualKeySchema, expectedKeySchema);
    }
  }

  @Test
  public void testGetKeySchemaWithoutPrimaryKey() throws IOException {
    try (InputStream schemaNoPkey = getClass().getClassLoader().getResourceAsStream("dedup-schema/dedup-schema-without-pkey.avsc")) {
      Schema topicSchema = new Schema.Parser().parse(schemaNoPkey);
      Schema actualKeySchema = this.runner.getKeySchema(this.job, topicSchema);
      Assert.assertEquals(actualKeySchema, AvroUtils.removeUncomparableFields(topicSchema).get());
    }
  }
}
