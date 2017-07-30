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

package gobblin.serde;

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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.serde.HiveSerDeConverter;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.hadoop.OldApiWritableFileExtractor;
import gobblin.source.extractor.hadoop.OldApiWritableFileSource;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;
import gobblin.writer.Destination;
import gobblin.writer.Destination.DestinationType;
import gobblin.writer.HiveWritableHdfsDataWriter;
import gobblin.writer.HiveWritableHdfsDataWriterBuilder;
import gobblin.writer.WriterOutputFormat;


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
