package gobblin.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Logger;


/**
 * Integration test for launching Gobblin via Oozie
 */
@Test
public class GobblinOozieTest {

  private MiniDFSCluster miniDFSCluster;
//  private MiniMRCluster miniMRCluster;
  private MiniYARNCluster miniYARNCluster;
  private OozieClient oozieClient;

  @BeforeClass
  public void setUp() throws Exception {
    System.out.println("Starting MiniDFSCluster");
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.security.authorization", "false");
    conf.set("dfs.block.access.token.enable", "false");

    String currentUser = System.getProperty("user.name");
    conf.set("hadoop.proxyuser." + currentUser + ".hosts", "*");
    conf.set("hadoop.proxyuser." + currentUser + ".groups", "*");
    conf.set("dfs.webhdfs.enabled", "true");

    this.miniDFSCluster = new MiniDFSCluster.Builder(conf).build();

    System.out.println("Starting MiniMRCluster");
    this.miniYARNCluster = new MiniYARNCluster("test", 1, 1, 1, 1);
    this.miniYARNCluster.init(conf);
    this.miniYARNCluster.start();

    String nameNode = new URI("hdfs://localhost:" + this.miniDFSCluster.getNameNodePort()).toString();
    String currentDir = System.getProperty("user.dir");

    File oozieHomeDir = new File(currentDir, "oozie-home-dir");
    oozieHomeDir.mkdirs();

    System.setProperty(Services.OOZIE_HOME_DIR, oozieHomeDir.toString());
//    this.miniDFSCluster.getFileSystem().mkdirs(new Path(new Path(nameNode, currentDir), "oozie-config-dir"));
    File configDir = new File(new Path(new Path(currentDir), "oozie-config-dir").toString());
    configDir.mkdirs();
    new File(currentDir, "oozie-config-dir/hadoop-conf").mkdirs();
    new File(currentDir, "oozie-config-dir/action-conf").mkdirs();
    System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, new Path(currentDir, "oozie-config-dir").toString());

    File oozieLibPath = new File(currentDir, "oozie-share-lib");
    System.setProperty(WorkflowAppService.SYSTEM_LIB_PATH, oozieLibPath.toString());
    System.setProperty(XLogService.LOG4J_FILE, "log4j.properties");
    System.setProperty("oozie.use.system.libpath", "true");
    System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "false");

    System.setProperty("oozie.db.schema.name", "oozie");
    System.setProperty("oozie.service.JPAService.create.db.schema", "true");
    System.setProperty("oozie.service.JPAService.validate.db.connection", "false");
    System.setProperty("oozie.service.JPAService.jdbc.driver", "org.hsqldb.jdbcDriver");
    System.setProperty("oozie.service.JPAService.jdbc.url", "jdbc:hsqldb:mem:oozie");
    System.setProperty("oozie.service.JPAService.jdbc.username", "sa");
    System.setProperty("oozie.service.JPAService.pool.max.active.conn", "10");

    System.setProperty("oozie.action.ship.launcher.jar", "true");

    System.out.println("Starting LocalOozie");
    LocalOozie.start();

    System.out.println("Getting Oozie Client");
    this.oozieClient = LocalOozie.getClient();
  }

  @Test
  public void testBasicJob() throws OozieClientException, IOException, InterruptedException, URISyntaxException {
    String nameNode = new URI("hdfs://localhost:" + this.miniDFSCluster.getNameNodePort()).toString();

    Properties props = oozieClient.createConfiguration();
    System.out.println(this.miniDFSCluster.getNameNode().getHostAndPort());
    props.setProperty("nameNode", new URI("hdfs://localhost:" + this.miniDFSCluster.getNameNodePort()).toString());
    System.out.println("tostring" + this.miniYARNCluster.getName());
    System.out.println("getname" + this.miniYARNCluster.getName());
    System.out.println(this.miniYARNCluster.getConfig().get("yarn.resourcemanager.address"));
    props.setProperty("jobTracker", this.miniYARNCluster.getConfig().get("yarn.resourcemanager.address"));
    props.setProperty("oozie.use.system.libpath", "true");
    props.setProperty("oozie.action.ship.launcher.jar", "true");
    props.setProperty("oozie.use.system.libpath", "true");

    String currentDir = System.getProperty("user.dir");
    File oozieLibPath = new File(currentDir, "oozie-share-lib");
    props.setProperty(WorkflowAppService.SYSTEM_LIB_PATH, oozieLibPath.toString());
    props.load(new FileInputStream(new File("gobblin-oozie/src/test/resources/local/gobblin-oozie-example-workflow.properties")));
    props.setProperty(OozieClient.APP_PATH, nameNode + "/gobblin-oozie/src/test/resources/local/");

    this.miniDFSCluster.getFileSystem().mkdirs(new Path("/gobblin-oozie/src/test/resources/local/"));
    this.miniDFSCluster.getFileSystem().copyFromLocalFile(new Path("gobblin-oozie/src/test/resources/local/gobblin-oozie-example-workflow.xml"), new Path(nameNode, "/gobblin-oozie/src/test/resources/local/workflow.xml"));

    props.setProperty("nameNode", new URI("hdfs://localhost:" + this.miniDFSCluster.getNameNodePort()).toString());
    props.setProperty("jobTracker", this.miniYARNCluster.getConfig().get("yarn.resourcemanager.address"));

    System.out.println("Running Oozie Workflow");
    String jobId = this.oozieClient.run(props);

//    this.oozieClient.getStatus(jobId);
  }

  @AfterClass
  public void tearDown() throws InterruptedException, IOException {
    this.miniDFSCluster.shutdown();
    this.miniYARNCluster.stop();
    this.miniYARNCluster.close();
    LocalOozie.stop();
  }
}
