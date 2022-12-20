package org.apache.gobblin.data.management.dataset;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.gobblin.data.management.copy.Manifest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestManifest {
  private FileSystem localFs;


  public TestManifest() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
  }

  @Test
  public void manifestSanityRead() throws IOException {
    //Get manifest Path
    String manifestPath =
        getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    Manifest manifest = Manifest.read(localFs, new Path(manifestPath));
    Assert.assertEquals(manifest._copyableUnits.size(), 2);
    Manifest.CopyableUnit cu = manifest._copyableUnits.get(0);
    int id = cu.id.intValue();
    Assert.assertEquals(id, 1);
    Assert.assertEquals(cu.fileName, "/tmp/dataset/test1.txt");
  }

  @Test
  public void manifestSanityWrite() throws IOException {
    File tmpDir = Files.createTempDir();
    Path output = new Path(tmpDir.getAbsolutePath(), "test");
    Manifest manifest = new Manifest();
    manifest.add(new Manifest.CopyableUnit(null, "testfilename", null, null, null));
    manifest.write(localFs, output);

    Manifest readManifest = Manifest.read(localFs, output);
    Assert.assertEquals(manifest._copyableUnits.size(), 1);
    Assert.assertEquals(manifest._copyableUnits.get(0).fileName, "testfilename");
  }

  @Test
  public void manifestSanityReadIterator() throws IOException {
    //Get manifest Path
    String manifestPath =
        getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    Manifest manifest = Manifest.read(localFs, new Path(manifestPath));

    Manifest.ManifestIterator manifestIterator = Manifest.getReadIterator(localFs, new Path(manifestPath));
    int count = 0;
    while (manifestIterator.hasNext()) {
      Manifest.CopyableUnit cu = manifestIterator.next();
      Assert.assertEquals(cu.fileName, manifest._copyableUnits.get(count).fileName);
      Assert.assertEquals(cu.id, manifest._copyableUnits.get(count).id);
      Assert.assertEquals(cu.fileGroup, manifest._copyableUnits.get(count).fileGroup);
      count++;
    }
  }
}
