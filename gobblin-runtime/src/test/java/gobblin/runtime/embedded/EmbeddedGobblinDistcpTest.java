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

package gobblin.runtime.embedded;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;


public class EmbeddedGobblinDistcpTest {

  @Test
  public void test() throws Exception {
    String fileName = "file";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    tmpFile.createNewFile();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.run();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());
  }

}
