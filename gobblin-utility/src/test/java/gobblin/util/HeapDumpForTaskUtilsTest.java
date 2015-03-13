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
  private Configuration conf;

  private static final String TEST_DIR = "./dumpScript/";
  private static final String SCRIPT_NAME = "dump.sh";

  @BeforeClass
  public void setUp() throws IOException {
    this.conf = new Configuration();
    this.conf.set("fs.default.name", "file:///");
    this.conf.set("mapred.job.tracker", "local");
    this.fs = FileSystem.getLocal(this.conf);
    this.fs.mkdirs(new Path(TEST_DIR));
  }

  @Test
  public void testGenerateDumpScript() throws IOException {
    Path dumpScript = new Path(TEST_DIR + SCRIPT_NAME);
    HeapDumpForTaskUtils.generateDumpScript(dumpScript, fs, "test.hprof", "chmod 777 ");
    Assert.assertEquals(true, fs.exists(dumpScript));
    Assert.assertEquals(true, fs.exists(new Path(dumpScript.getParent() + "/dumps/")));
    Closer closer = Closer.create();
    BufferedReader scriptReader = closer.register(new BufferedReader(new InputStreamReader(fs.open(dumpScript))));
    Assert.assertEquals("#!/bin/sh", scriptReader.readLine());
    Assert.assertEquals("hadoop dfs -put test.hprof dumpScript/dumps/${PWD//\\//_}.hprof", scriptReader.readLine());
  }

  @AfterClass
  public void tearDown() throws IOException {
    fs.delete(new Path(TEST_DIR), true);
    fs.close();
  }
}
