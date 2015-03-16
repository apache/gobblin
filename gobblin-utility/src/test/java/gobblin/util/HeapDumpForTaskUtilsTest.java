/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;


@Test(groups = { "gobblin.util" })
public class HeapDumpForTaskUtilsTest {

  private FileSystem fs;

  private static final String TEST_DIR = "dumpScript";
  private static final String SCRIPT_NAME = "dump.sh";

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.fs.mkdirs(new Path(TEST_DIR));
  }

  @Test
  public void testGenerateDumpScript() throws IOException {
    Path dumpScript = new Path(TEST_DIR, SCRIPT_NAME);
    HeapDumpForTaskUtils.generateDumpScript(dumpScript, this.fs, "test.hprof", "chmod 777 ");
    Assert.assertEquals(true, this.fs.exists(dumpScript));
    Assert.assertEquals(true, this.fs.exists(new Path(dumpScript.getParent(), "dumps")));
    Closer closer = Closer.create();
    try {
      BufferedReader scriptReader =
          closer.register(new BufferedReader(new InputStreamReader(this.fs.open(dumpScript))));
      Assert.assertEquals("#!/bin/sh", scriptReader.readLine());
      Assert.assertEquals("if [ -n \"$HADOOP_PREFIX\" ]; then", scriptReader.readLine());
      Assert.assertEquals("  ${HADOOP_PREFIX}/bin/hadoop dfs -put test.hprof dumpScript/dumps/${PWD//\\//_}.hprof",
          scriptReader.readLine());
      Assert.assertEquals("else", scriptReader.readLine());
      Assert.assertEquals("  ${HADOOP_HOME}/bin/hadoop dfs -put test.hprof dumpScript/dumps/${PWD//\\//_}.hprof",
          scriptReader.readLine());
      Assert.assertEquals("fi", scriptReader.readLine());
    } catch (Throwable t) {
      closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    fs.delete(new Path(TEST_DIR), true);
    fs.close();
  }
}
