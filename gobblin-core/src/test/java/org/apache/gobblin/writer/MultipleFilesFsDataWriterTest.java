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
package org.apache.gobblin.writer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import gobblin.configuration.State;

import static org.apache.gobblin.writer.MultipleFilesFsDataWriter.WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Test(groups = {"gobblin.writer"})
public class MultipleFilesFsDataWriterTest {
  private MultipleFilesFsDataWriterBuilder<Object, Integer> builder;
  private MultipleFilesFsDataWriter<Integer> writer;
  private State state;

  @BeforeMethod
  public void setUp()
      throws IOException {
    builder = mock(MultipleFilesFsDataWriterBuilder.class);
    state = createStateWithConfig();
    System.out.println(state);
    Answer<Object> buildIntWriterWithProvidedPath = i -> new FileFormatIntWriter((Path) i.getArguments()[1]);
    when(builder.getNewWriter(anyInt(), any(Path.class))).thenAnswer(buildIntWriterWithProvidedPath);
    when(builder.getFileName(any(State.class))).thenReturn(TestConstants.TEST_FILE_NAME);
    writer = new MultipleFilesFsDataWriter<Integer>(builder, state) {

      @Override
      public void writeWithCurrentWriter(FileFormatWriter<Integer> writer, Integer record)
          throws IOException {
        writer.write(record);
      }

      @Override
      public void closeCurrentWriter(FileFormatWriter<Integer> writer)
          throws IOException {
        writer.close();
      }
    };

    // Making the staging and/or output dirs if necessary
    File stagingDir = new File(buildAbsolutePath(TestConstants.TEST_STAGING_DIR));
    File outputDir = new File(buildAbsolutePath(TestConstants.TEST_OUTPUT_DIR));
    File writerPath = new File(buildAbsolutePath(Paths.get(TestConstants.TEST_STAGING_DIR, getFilePath()).toString()));
    if (!stagingDir.exists()) {
      boolean mkdirs = stagingDir.mkdirs();
      assertAndLog(mkdirs, stagingDir);
    }
    if (!outputDir.exists()) {
      boolean mkdirs = outputDir.mkdirs();
      assertAndLog(mkdirs, outputDir);
    }
    if (!writerPath.exists()) {
      boolean mkdirs = writerPath.mkdirs();
      assertAndLog(mkdirs, writerPath);
    }
  }

  @Test
  public void testIfWritersAreSwitchedWhenRecordThresholdReached()
      throws IOException {
    for (int i = 0; i < WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT; i++) {
      writer.write(i);
    }
    assertThat(writer.recordsWritten(), is(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT));
    writer.write(Integer.valueOf(String.valueOf(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT + 1)));
    assertThat(writer.recordsWritten(), is(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT + 1));
    writer.close();
    verify(builder, Mockito.times(2)).getNewWriter(anyInt(), any(Path.class));
  }

  @Test
  public void testIfStageFilesAreMovedToOutputOnCommit()
      throws IOException {

    for (int i = 0; i < WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT; i++) {
      writer.write(i);
    }
    assertThat(writer.recordsWritten(), is(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT));
    writer.write(Integer.valueOf(String.valueOf(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT + 1)));
    assertThat(writer.recordsWritten(), is(WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT + 1));
    writer.close();
    writer.commit();
    verify(builder, Mockito.times(2)).getNewWriter(anyInt(), any(Path.class));
  }

  @AfterMethod
  public void tearDown() {
    // Clean up the staging and/or output directories if necessary
    String testDir = buildAbsolutePath(TestConstants.TEST_ROOT_DIR);
    File testRootDir = new File(testDir);
    if (testRootDir.exists()) {
      FileUtil.fullyDelete(testRootDir);
    }
  }

  private State createStateWithConfig() {
    State properties = new State();
    properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, buildAbsolutePath(TestConstants.TEST_STAGING_DIR));
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, buildAbsolutePath(TestConstants.TEST_OUTPUT_DIR));
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, getFilePath());
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestConstants.TEST_FILE_NAME);
    return properties;
  }

  private String buildAbsolutePath(String path) {
    return Paths.get(System.getProperty("java.io.tmpdir"), path).toAbsolutePath().toString();
  }

  private String getFilePath() {
    return TestConstants.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" + TestConstants.TEST_EXTRACT_TABLE + "/"
        + TestConstants.TEST_EXTRACT_ID + "_" + TestConstants.TEST_EXTRACT_PULL_TYPE;
  }

  private class FileFormatIntWriter implements FileFormatWriter<Integer> {
    private final Path filePath;

    private FileFormatIntWriter(Path filePath) {
      this.filePath = filePath;
    }

    @Override
    public void write(Integer record)
        throws IOException {
      java.nio.file.Path path = Paths.get(this.filePath.toString());
      File file = new File(path.toString());
      file.createNewFile();
      Files.write(path, record.toString().getBytes(), StandardOpenOption.APPEND);
    }

    @Override
    public void close() {

    }
  }

  private void assertAndLog(boolean mkdirs, File file) {
    assert (mkdirs) : "Could not create directory " + file.toPath().toString();
  }
}