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

package org.apache.gobblin.runtime.spec_store;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.spec_catalog.FlowCatalogTest.*;


public class FSSpecStoreTest {

  @Test
  public void testPathConversion() throws Exception {
    Properties properties = new Properties();
    File tmpDir = Files.createTempDir();
    properties.setProperty(FSSpecStore.SPECSTORE_FS_DIR_KEY, tmpDir.getAbsolutePath());
    SpecSerDe specSerDe = Mockito.mock(SpecSerDe.class);
    FSSpecStore fsSpecStore = new FSSpecStore(ConfigUtils.propertiesToConfig(properties), specSerDe);

    Path rootPath = new Path("/a/b/c");
    URI uri = URI.create("ddd");
    Assert.assertEquals(fsSpecStore.getURIFromPath(fsSpecStore.getPathForURI(rootPath, uri, ""), rootPath), uri);
  }

  /**
   * Make sure that when there's on spec failed to be deserialized, the rest of spec in specStore can
   * still be taken care of.
   */
  @Test
  public void testGetSpecRobustness() throws Exception {

    File specDir = Files.createTempDir();
    Properties properties = new Properties();
    properties.setProperty(FSSpecStore.SPECSTORE_FS_DIR_KEY, specDir.getAbsolutePath());
    SpecSerDe serde = Mockito.mock(SpecSerDe.class);
    TestFsSpecStore fsSpecStore = new TestFsSpecStore(ConfigUtils.propertiesToConfig(properties), serde);

    // Version is specified as 0,1,2
    File specFileFail = new File(specDir, "spec_fail");
    Assert.assertTrue(specFileFail.createNewFile());
    File specFile1 = new File(specDir, "spec0");
    Assert.assertTrue(specFile1.createNewFile());
    File specFile2 = new File(specDir, "spec1");
    Assert.assertTrue(specFile2.createNewFile());
    File specFile3 = new File(specDir, "serDeFail");
    Assert.assertTrue(specFile3.createNewFile());

    FileSystem fs = FileSystem.getLocal(new Configuration());
    Assert.assertEquals(fs.getFileStatus(new Path(specFile3.getAbsolutePath())).getLen(), 0);

    Collection<Spec> specList = fsSpecStore.getSpecs();
    // The fail and serDe datasets wouldn't survive
    Assert.assertEquals(specList.size(), 2);
    for (Spec spec: specList) {
      Assert.assertFalse(spec.getDescription().contains("spec_fail"));
      Assert.assertFalse(spec.getDescription().contains("serDeFail"));
    }
  }

  class TestFsSpecStore extends FSSpecStore {
    public TestFsSpecStore(Config sysConfig, SpecSerDe specSerDe) throws IOException {
      super(sysConfig, specSerDe);
    }

    @Override
    protected Spec readSpecFromFile(Path path) throws IOException {
      if (path.getName().contains("fail")) {
        throw new IOException("Mean to fail in the test");
      } else if (path.getName().contains("serDeFail")) {

        // Simulate the way that a serDe exception
        FSDataInputStream fis = fs.open(path);
        SerializationUtils.deserialize(ByteStreams.toByteArray(fis));

        // This line should never be reached since we generate SerDe Exception on purpose.
        Assert.fail();
        return null;
      }
      else return initFlowSpec(Files.createTempDir().getAbsolutePath());
    }
  }


  @Test
  public void testGetSpecURI() throws Exception {
    File specDir = Files.createTempDir();
    Properties properties = new Properties();
    properties.setProperty(FSSpecStore.SPECSTORE_FS_DIR_KEY, specDir.getAbsolutePath());
    SpecSerDe serde = Mockito.mock(SpecSerDe.class);
    FSSpecStore fsSpecStore = new FSSpecStore(ConfigUtils.propertiesToConfig(properties), serde);

    URI specURI0 = URI.create("spec0");
    URI specURI1 = URI.create("spec1");
    URI specURI2 = URI.create("spec2");

    File specFile1 = new File(specDir, "spec0");
    Assert.assertTrue(specFile1.createNewFile());
    File specFile2 = new File(specDir, "spec1");
    Assert.assertTrue(specFile2.createNewFile());
    File specFile3 = new File(specDir, "spec2");
    Assert.assertTrue(specFile3.createNewFile());

    fsSpecStore.exists(specURI0);
    fsSpecStore.exists(specURI1);
    fsSpecStore.exists(specURI2);

    Iterator<URI> it = fsSpecStore.getSpecURIs();
    int count = 0;
    List<URI> result = new ArrayList<>();
    while (it.hasNext()) {
      count += 1 ;
      result.add(it.next());
    }

    Assert.assertEquals(count, 3);
    Assert.assertEquals(fsSpecStore.getSize(), 3);
    Assert.assertTrue(result.contains(specURI0));
    Assert.assertTrue(result.contains(specURI1));
    Assert.assertTrue(result.contains(specURI2));
  }
}