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

package gobblin.commit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.State;


/**
 * Test for {@link FsRenameCommitStep}.
 *
 * @author Ziyang Liu
 */
@Test(groups = { "gobblin.commit" })
public class FsRenameCommitStepTest {
  private static final String ROOT_DIR = "fs-rename-commit-sequence-test";

  private FileSystem fs;
  private FsRenameCommitStep step;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.fs.delete(new Path(ROOT_DIR), true);

    Path dir1 = new Path(ROOT_DIR, "dir1");
    Path dir2 = new Path(ROOT_DIR, "dir2");

    this.fs.mkdirs(dir1);
    this.fs.mkdirs(dir2);

    Path src = new Path(dir1, "file");
    Path dst = new Path(dir2, "file");
    this.fs.createNewFile(src);

    this.step =
        (FsRenameCommitStep) new FsRenameCommitStep.Builder<>().from(src).to(dst).withProps(new State()).build();
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.fs.delete(new Path(ROOT_DIR), true);
  }

  @Test
  public void testExecute() throws IOException {
    this.step.execute();
    Assert.assertTrue(this.fs.exists(new Path(ROOT_DIR, "dir2/file")));
  }
}
