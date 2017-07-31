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

package org.apache.gobblin.serde;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.serde.HiveSerDeConverter;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.hadoop.OldApiWritableFileExtractor;
import org.apache.gobblin.source.extractor.hadoop.OldApiWritableFileSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.Destination.DestinationType;
import org.apache.gobblin.writer.HiveWritableHdfsDataWriter;
import org.apache.gobblin.writer.HiveWritableHdfsDataWriterBuilder;
import org.apache.gobblin.writer.WriterOutputFormat;


/**
 * Unit test for data ingestion using Hive SerDes.
 *
 * @author Ziyang Liu
 */
public class HiveSerDeTest {

  private FileSystem fs;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.get(new Configuration());
  }

  /**
   * This test uses Avro SerDe to deserialize data from Avro files, and use ORC SerDe
   * to serialize them into ORC files.
   */
  @Test(groups = { "gobblin.serde" })
  public void testAvroOrcSerDes()
      throws IOException, DataRecordException, DataConversionException {
    Properties properties = new Properties();
    properties.load(new FileReader("gobblin-core/src/test/resources/serde/serde.properties"));
    SourceState sourceState = new SourceState(new State(properties), ImmutableList.<WorkUnitState> of());

    OldApiWritableFileSource source = new OldApiWritableFileSource();
    List<WorkUnit> workUnits = source.getWorkunits(sourceState);

    Assert.assertEquals(workUnits.size(), 1);

    WorkUnitState wus = new WorkUnitState(workUnits.get(0));
    wus.addAll(sourceState);

    Closer closer = Closer.create();

    HiveWritableHdfsDataWriter writer = null;
    try {
      OldApiWritableFileExtractor extractor = closer.register((OldApiWritableFileExtractor) source.getExtractor(wus));
      HiveSerDeConverter converter = closer.register(new HiveSerDeConverter());
      writer =
          closer.register((HiveWritableHdfsDataWriter) new HiveWritableHdfsDataWriterBuilder<>().withBranches(1)
              .withWriterId("0").writeTo(Destination.of(DestinationType.HDFS, sourceState))
              .writeInFormat(WriterOutputFormat.ORC).build());

      converter.init(wus);
      Writable record;

      while ((record = extractor.readRecord(null)) != null) {
        Iterable<Writable> convertedRecordIterable = converter.convertRecordImpl(null, record, wus);
        Assert.assertEquals(Iterators.size(convertedRecordIterable.iterator()), 1);
        writer.write(convertedRecordIterable.iterator().next());
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
      if (writer != null) {
        writer.commit();
      }
      Assert.assertTrue(this.fs.exists(new Path(sourceState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
          sourceState.getProp(ConfigurationKeys.WRITER_FILE_NAME))));
      HadoopUtils.deletePath(this.fs, new Path(sourceState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR)), true);
    }
  }
}
