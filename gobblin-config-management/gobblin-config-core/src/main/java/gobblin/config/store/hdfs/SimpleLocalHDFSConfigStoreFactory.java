package gobblin.config.store.hdfs;

import org.apache.hadoop.fs.Path;


/**
 * Extension of {@link SimpleHDFSConfigStoreFactory} that creates a {@link SimpleHDFSConfigStore} which works for the
 * local file system.
 */
public class SimpleLocalHDFSConfigStoreFactory extends SimpleHDFSConfigStoreFactory {

  private static final String LOCAL_HDFS_SCHEME_NAME = "file";

  @Override
  protected String getPhysicalScheme() {
    return LOCAL_HDFS_SCHEME_NAME;
  }

  @Override
  protected Path getDefaultRootDir() {
    return new Path(System.getProperty("user.dir"));
  }
}
