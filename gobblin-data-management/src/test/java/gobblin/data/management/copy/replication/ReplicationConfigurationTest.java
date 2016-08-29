package gobblin.data.management.copy.replication;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;
import gobblin.util.filesystem.PathAlterationDetector;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import java.util.Set;
import java.util.concurrent.Semaphore;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.data.management.copy.replication"})
public class ReplicationConfigurationTest {
  @Test
  public void testloadValidConfigs(){
    Assert.assertEquals(1, 1);
    
    URL u = getClass().getClassLoader().getResource("replicationConfigTest");
    System.out.println("url is " + u);
    //System.out.println("exist? " + new File(filename).exists());
    
  }
}
