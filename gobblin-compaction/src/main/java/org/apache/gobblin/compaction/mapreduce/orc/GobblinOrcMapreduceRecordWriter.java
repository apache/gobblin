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

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.StripeInformation;
import org.apache.orc.Writer;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.reflection.RestrictedFieldAccessingUtils;


/**
 * A thin extension to {@link OrcMapreduceRecordWriter} for obtaining a vector of stripe information.
 */
@Slf4j
public class GobblinOrcMapreduceRecordWriter extends OrcMapreduceRecordWriter {
  public GobblinOrcMapreduceRecordWriter(Writer writer) {
    super(writer);
  }

  public GobblinOrcMapreduceRecordWriter(Writer writer, int rowBatchSize) {
    super(writer, rowBatchSize);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException {
    super.close(taskAttemptContext);

    // TODO: Emit this information as kafka events for ease for populating dashboard.
    try {
      String stripeSizeVec = ((Writer) RestrictedFieldAccessingUtils.getRestrictedFieldByReflection(
        this, "writer", this.getClass())).getStripes()
          .stream()
          .mapToLong(StripeInformation::getDataLength).mapToObj(String::valueOf)
          .reduce((x,y) -> x.concat(",").concat(y)).get();
      log.info("The vector of Stripe-Size in enclosing writer is:" + stripeSizeVec);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      log.error("Failed to access writer object from super class to obtain stripe information");
    }
  }
}
