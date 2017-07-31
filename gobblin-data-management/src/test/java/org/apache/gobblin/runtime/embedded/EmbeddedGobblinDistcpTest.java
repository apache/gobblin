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

package org.apache.gobblin.runtime.embedded;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.client.util.Charsets;
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

    FileOutputStream os = new FileOutputStream(tmpFile);
    for (int i = 0; i < 100; i++) {
      os.write("myString".getBytes(Charsets.UTF_8));
    }
    os.close();

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
