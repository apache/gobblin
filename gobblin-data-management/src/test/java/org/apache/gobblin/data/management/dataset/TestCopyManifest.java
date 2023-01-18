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

package org.apache.gobblin.data.management.dataset;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.gobblin.data.management.copy.CopyManifest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCopyManifest {
  private FileSystem localFs;


  public TestCopyManifest() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
  }

  @Test
  public void manifestSanityRead() throws IOException {
    //Get manifest Path
    String manifestPath =
        getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    CopyManifest manifest = CopyManifest.read(localFs, new Path(manifestPath));
    Assert.assertEquals(manifest._copyableUnits.size(), 2);
    CopyManifest.CopyableUnit cu = manifest._copyableUnits.get(0);
    Assert.assertEquals(cu.fileName, "/tmp/dataset/test1.txt");
  }

  @Test
  public void manifestSanityWrite() throws IOException {
    File tmpDir = Files.createTempDir();
    Path output = new Path(tmpDir.getAbsolutePath(), "test");
    CopyManifest manifest = new CopyManifest();
    manifest.add(new CopyManifest.CopyableUnit("testfilename", null, null, null));
    manifest.write(localFs, output);

    CopyManifest readManifest = CopyManifest.read(localFs, output);
    Assert.assertEquals(readManifest._copyableUnits.size(), 1);
    Assert.assertEquals(readManifest._copyableUnits.get(0).fileName, "testfilename");
  }

  @Test
  public void manifestSanityReadIterator() throws IOException {
    //Get manifest Path
    String manifestPath =
        getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    CopyManifest manifest = CopyManifest.read(localFs, new Path(manifestPath));

    CopyManifest.CopyableUnitIterator manifestIterator = CopyManifest.getReadIterator(localFs, new Path(manifestPath));
    int count = 0;
    while (manifestIterator.hasNext()) {
      CopyManifest.CopyableUnit cu = manifestIterator.next();
      Assert.assertEquals(cu.fileName, manifest._copyableUnits.get(count).fileName);
      Assert.assertEquals(cu.fileGroup, manifest._copyableUnits.get(count).fileGroup);
      count++;
    }
    Assert.assertEquals(count, 2);
    Assert.assertEquals(count, manifest._copyableUnits.size());
    manifestIterator.close();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void invalidCopyableUnit() {
    new CopyManifest.CopyableUnit(null, null, null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void invalidReadIteratorCopyManifest() throws IOException {
    String manifestPath =
        getClass().getClassLoader().getResource("manifestBasedDistcpTest/missingFileNameManifest.json").getPath();
    CopyManifest.CopyableUnitIterator manifestIterator = CopyManifest.getReadIterator(localFs, new Path(manifestPath));
    manifestIterator.next();
  }
}
