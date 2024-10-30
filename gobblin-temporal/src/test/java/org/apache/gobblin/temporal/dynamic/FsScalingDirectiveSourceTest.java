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

package org.apache.gobblin.temporal.dynamic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import org.apache.gobblin.util.io.SeekableFSInputStream;


public class FsScalingDirectiveSourceTest {

  private static final String DIRECTIVES_DIR = "/test/dynamic/directives";
  private static final String ERRORS_DIR = "/test/dynamic/errors";
  private FileSystem fileSystem;
  private FsScalingDirectiveSource source;
  private static final ScalingDirectiveParser parser = new ScalingDirectiveParser();

  @BeforeMethod
  public void setUp() {
    fileSystem = Mockito.mock(FileSystem.class);
    source = new FsScalingDirectiveSource(fileSystem, DIRECTIVES_DIR, Optional.of(ERRORS_DIR));
  }

  @Test
  public void getScalingDirectivesWhenAllValidFiles() throws IOException, ScalingDirectiveParser.InvalidSyntaxException {
    String[] fileNames = {
        "1700010000.=4",
        "1700020000.new_profile=7,+(a.b.c=7,x.y=five)",
        "1700030000.another_profile=3,+(a.b.c=8,x.y=six)",
        "1700040000.new_profile=2"
    };
    FileStatus[] fileStatuses = Streams.mapWithIndex(Arrays.stream(fileNames), (fileName, i) ->
        createFileStatus(fileName, 1000 * (i + 1))
        ).toArray(FileStatus[]::new);
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    List<ScalingDirective> directives = source.getScalingDirectives();

    Assert.assertEquals(directives.size(), 4);
    for (int i = 0; i < directives.size(); i++) {
      Assert.assertEquals(directives.get(i), parseDirective(fileNames[i]), "fileNames[" + i + "] = " + fileNames[i]);
    }
  }

  @Test
  public void getScalingDirectivesWhileIgnoringInvalidEntries() throws IOException, ScalingDirectiveParser.InvalidSyntaxException {
    String[] fileNames = {
        "1700010000.=4",
        // still returned... although it would later be rejected as `WorkforcePlan.IllegalRevisionException.UnrecognizedProfile`
        "1700020000.new_profile=2",
        "1700030000.new_profile=7,+(a.b.c=7,x.y=five)",
        // rejected: illegal syntax will fail to parse
        "completely invalid",
        "1700040000.another_profile=3,+(a.b.c=8,x.y=six)",
        // rejected: because we later mock this as a dir, but a directive MUST be a file
        "1700046000.acutally_a_dir=6,-(b.a,y.x)",
        "1700050000.new_profile=9",
        // rejected: because Removing must list key names, NOT key-value pairs
        "1700055000.bad_directive=69,my_profile-(x=y,z=1)"
    };
    FileStatus[] fileStatuses = Streams.mapWithIndex(Arrays.stream(fileNames), (fileName, i) -> {
          boolean isFile = !fileName.contains("_a_dir=");
          return createFileStatus(fileName, 1000 * (i + 1), isFile);
        }).toArray(FileStatus[]::new);
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    List<ScalingDirective> directives = source.getScalingDirectives();

    Assert.assertEquals(directives.size(), 5);
    Assert.assertEquals(directives.get(0), parseDirective(fileNames[0]), "fileNames[" + 0 + "] = " + fileNames[0]);
    Assert.assertEquals(directives.get(1), parseDirective(fileNames[1]), "fileNames[" + 1 + "] = " + fileNames[1]);
    Assert.assertEquals(directives.get(2), parseDirective(fileNames[2]), "fileNames[" + 2 + "] = " + fileNames[2]);
    Assert.assertEquals(directives.get(3), parseDirective(fileNames[4]), "fileNames[" + 4 + "] = " + fileNames[4]);
    Assert.assertEquals(directives.get(4), parseDirective(fileNames[6]), "fileNames[" + 6 + "] = " + fileNames[6]);

    // lastly, verify `ERRORS_DIR` acknowledgements (i.e. FS object rename) work as expected:
    ArgumentCaptor<Path> sourcePathCaptor = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Path> destPathCaptor = ArgumentCaptor.forClass(Path.class);
    Mockito.verify(fileSystem, Mockito.times(fileNames.length - directives.size()))
        .rename(sourcePathCaptor.capture(), destPathCaptor.capture());

    List<String> expectedErrorFileNames = Lists.newArrayList(fileNames[3], fileNames[5], fileNames[7]);
    List<Path> expectedErrorDirectivePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(DIRECTIVES_DIR, fileName))
        .collect(Collectors.toList());
    List<Path> expectedErrorPostRenamePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(ERRORS_DIR, fileName))
        .collect(Collectors.toList());

    Assert.assertEquals(sourcePathCaptor.getAllValues(), expectedErrorDirectivePaths);
    Assert.assertEquals(destPathCaptor.getAllValues(), expectedErrorPostRenamePaths);
  }

  @Test
  public void getScalingDirectivesWhileIgnoringOutOfOrderEntries() throws IOException, ScalingDirectiveParser.InvalidSyntaxException {
    String[] fileNames = {
        "1700010000.=4",
        "1700030000.new_profile=7,+(a.b.c=7,x.y=five)",
        "1700040000.another_profile=3,+(a.b.c=8,x.y=six)",
        "1700050000.new_profile=9"
    };
    FileStatus[] fileStatuses = Streams.mapWithIndex(Arrays.stream(fileNames), (fileName, i) ->
        // NOTE: elements [1] and [3] modtime will be 0, making them out of order against their directive timestamp (in their filename, like `1700030000.`)
        createFileStatus(fileName, 1000 * (i + 1) * ((i + 1) % 2))
    ).toArray(FileStatus[]::new);
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    List<ScalingDirective> directives = source.getScalingDirectives();

    Assert.assertEquals(directives.size(), 2);
    Assert.assertEquals(directives.get(0), parseDirective(fileNames[0]), "fileNames[" + 0 + "] = " + fileNames[0]);
    Assert.assertEquals(directives.get(1), parseDirective(fileNames[2]), "fileNames[" + 2 + "] = " + fileNames[2]);

    // lastly, verify `ERRORS_DIR` acknowledgements (i.e. FS object rename) work as expected:
    ArgumentCaptor<Path> sourcePathCaptor = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Path> destPathCaptor = ArgumentCaptor.forClass(Path.class);
    Mockito.verify(fileSystem, Mockito.times(fileNames.length - directives.size()))
        .rename(sourcePathCaptor.capture(), destPathCaptor.capture());

    List<String> expectedErrorFileNames = Lists.newArrayList(fileNames[1], fileNames[3]);
    List<Path> expectedErrorDirectivePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(DIRECTIVES_DIR, fileName))
        .collect(Collectors.toList());
    List<Path> expectedErrorPostRenamePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(ERRORS_DIR, fileName))
        .collect(Collectors.toList());

    Assert.assertEquals(sourcePathCaptor.getAllValues(), expectedErrorDirectivePaths);
    Assert.assertEquals(destPathCaptor.getAllValues(), expectedErrorPostRenamePaths);
  }

  @Test
  public void getScalingDirectivesWithOverlayPlaceholders() throws IOException, ScalingDirectiveParser.InvalidSyntaxException {
    String[] fileNames = {
        "1700010000.=4",
        "1700020000.some_profile=9,+(...)",
        "1700030000.other_profile=2,-(...)",
        "1700040000.some_profile=3",
        "1700050000.other_profile=10"
    };
    String addingOverlayDef = "a.b.c=7,x.y=five"; // for [1]
    String removingOverlayDef = "b.c,y.z.a"; // for [2]
    FileStatus[] fileStatuses = Streams.mapWithIndex(Arrays.stream(fileNames), (fileName, i) ->
        createFileStatus(fileName, 1000 * (i + 1))
    ).toArray(FileStatus[]::new);
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    Mockito.when(fileSystem.open(new Path(DIRECTIVES_DIR, fileNames[1]))).thenReturn(createInputStreamFromString(addingOverlayDef));
    Mockito.when(fileSystem.open(new Path(DIRECTIVES_DIR, fileNames[2]))).thenReturn(createInputStreamFromString(removingOverlayDef));
    List<ScalingDirective> directives = source.getScalingDirectives();

    Assert.assertEquals(directives.size(), fileNames.length);
    Assert.assertEquals(directives.get(0), parseDirective(fileNames[0]), "fileNames[" + 0 + "] = " + fileNames[0]);
    Assert.assertEquals(directives.get(1), parseDirective(fileNames[1].replace("...", addingOverlayDef)), "fileNames[" + 1 + "] = " + fileNames[1]);
    Assert.assertEquals(directives.get(2), parseDirective(fileNames[2].replace("...", removingOverlayDef)), "fileNames[" + 2 + "] = " + fileNames[2]);
    Assert.assertEquals(directives.get(3), parseDirective(fileNames[3]), "fileNames[" + 3 + "] = " + fileNames[3]);
    Assert.assertEquals(directives.get(4), parseDirective(fileNames[4]), "fileNames[" + 4 + "] = " + fileNames[4]);

    Mockito.verify(fileSystem, Mockito.never()).rename(Mockito.any(), Mockito.any());
  }

  @Test
  public void getScalingDirectivesWithOverlayPlaceholdersButInvalidDefinitions() throws IOException, ScalingDirectiveParser.InvalidSyntaxException {
    String[] fileNames = {
        "1700020000.some_profile=9,+(...)",
        "1700030000.other_profile=2,-(...)",
        "1700070000.=10"
    };
    // NOTE: switch these, so the overlay defs are invalid from `addingOverlayDef` with Removing and `removingOverlayDef` with Adding
    String addingOverlayDef = "a.b.c=7,x.y=five"; // for [1]
    String removingOverlayDef = "b.c,y.z.a"; // for [0]
    FileStatus[] fileStatuses = Streams.mapWithIndex(Arrays.stream(fileNames), (fileName, i) ->
        createFileStatus(fileName, 1000 * (i + 1))
    ).toArray(FileStatus[]::new);
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    Mockito.when(fileSystem.open(new Path(DIRECTIVES_DIR, fileNames[0]))).thenReturn(createInputStreamFromString(removingOverlayDef));
    Mockito.when(fileSystem.open(new Path(DIRECTIVES_DIR, fileNames[1]))).thenReturn(createInputStreamFromString(addingOverlayDef));
    List<ScalingDirective> directives = source.getScalingDirectives();

    Assert.assertEquals(directives.size(), 1);
    Assert.assertEquals(directives.get(0), parseDirective(fileNames[2]), "fileNames[" + 2 + "] = " + fileNames[2]);

    // lastly, verify `ERRORS_DIR` acknowledgements (i.e. FS object rename) work as expected:
    ArgumentCaptor<Path> sourcePathCaptor = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Path> destPathCaptor = ArgumentCaptor.forClass(Path.class);
    Mockito.verify(fileSystem, Mockito.times(fileNames.length - directives.size()))
        .rename(sourcePathCaptor.capture(), destPathCaptor.capture());

    List<String> expectedErrorFileNames = Lists.newArrayList(fileNames[0], fileNames[1]);
    List<Path> expectedErrorDirectivePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(DIRECTIVES_DIR, fileName))
        .collect(Collectors.toList());
    List<Path> expectedErrorPostRenamePaths = expectedErrorFileNames.stream()
        .map(fileName -> new Path(ERRORS_DIR, fileName))
        .collect(Collectors.toList());

    Assert.assertEquals(sourcePathCaptor.getAllValues(), expectedErrorDirectivePaths);
    Assert.assertEquals(destPathCaptor.getAllValues(), expectedErrorPostRenamePaths);
  }

  @Test
  public void getScalingDirectivesWithNoFiles() throws IOException {
    FileStatus[] fileStatuses = {};
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenReturn(fileStatuses);
    Assert.assertTrue(source.getScalingDirectives().isEmpty());
  }

  @Test(expectedExceptions = IOException.class)
  public void getScalingDirectivesWithIOException() throws IOException {
    Mockito.when(fileSystem.listStatus(new Path(DIRECTIVES_DIR))).thenThrow(new IOException());
    source.getScalingDirectives();
  }

  protected static ScalingDirective parseDirective(String s) throws ScalingDirectiveParser.InvalidSyntaxException {
    return parser.parse(s);
  }

  protected static FileStatus createFileStatus(String fileName, long modTime) {
    return createFileStatus(fileName, modTime, true);
  }

  protected static FileStatus createFileStatus(String fileName, long modTime, boolean isFile) {
    return new FileStatus(0, !isFile, 0, 0, modTime, new Path(DIRECTIVES_DIR, fileName));
  }

  public static FSDataInputStream createInputStreamFromString(String input) {
    return new FSDataInputStream(new SeekableFSInputStream(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))));
  }
}