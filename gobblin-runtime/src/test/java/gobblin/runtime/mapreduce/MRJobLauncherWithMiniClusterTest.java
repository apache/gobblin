package gobblin.runtime.mapreduce;

import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;


@Test(groups = { "gobblin.runtime.mapreduce" })
public class MRJobLauncherWithMiniClusterTest {

  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;

  @BeforeClass
  public void setUp() throws IOException {
    System.out.println("Starting MiniDFSCluster");
    Configuration conf = new Configuration();
    this.miniDFSCluster = new MiniDFSCluster.Builder(conf).build();
    System.out.println("Starting MiniMRCluster");
    this.miniMRCluster = new MiniMRCluster(1, "hdfs://localhost:" + this.miniDFSCluster.getNameNodePort(), 1);
  }

  @Test
  public void test() throws IOException {
    OutputStream outputStream = this.miniDFSCluster.getFileSystem().create(new Path("/test.json"));
    IOUtils.write("Hello World", outputStream);
    outputStream.close();

    InputStream inputStream = this.miniDFSCluster.getFileSystem().open(new Path("/test.json"));
    List<String> lines = IOUtils.readLines(inputStream, Charsets.UTF_8);

    System.out.println(Arrays.toString(lines.toArray()));
  }

  @AfterClass
  public void tearDown() {
    if (this.miniDFSCluster != null) {
      this.miniDFSCluster.shutdown();
    }
    if (this.miniMRCluster != null) {
      this.miniMRCluster.shutdown();
    }
  }
}
