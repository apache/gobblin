package gobblin.config.store.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.ConfigStoreFactory;


/**
 * An implementation of {@link ConfigStoreFactory} for creating {@link SimpleHDFSConfigStore}s. This class only works
 * the physical scheme {@link #HDFS_SCHEME_NAME}.
 *
 * @see {@link SimpleHDFSConfigStore}
 */
public class SimpleHDFSConfigStoreFactory implements ConfigStoreFactory<SimpleHDFSConfigStore> {

  protected static final String SIMPLE_HDFS_SCHEME_PREFIX = "simple-";
  protected static final String HDFS_SCHEME_NAME = "hdfs";

  @Override
  public String getScheme() {
    return SIMPLE_HDFS_SCHEME_PREFIX + getPhysicalScheme();
  }

  /**
   * Creates a {@link SimpleHDFSConfigStore} for the given {@link URI}. The {@link URI} specified should be the fully
   * qualified path to the dataset in question. For example,
   * {@code simple-hdfs://[authority]:[port][path-to-config-store][path-to-dataset]}. It is important to note that the
   * path to the config store on HDFS must also be specified. The combination
   * {@code [path-to-config-store][path-to-dataset]} need not specify an actual {@link Path} on HDFS.
   *
   * <p>
   *   If the {@link URI} does not contain an authority, a default authority and root directory are provided. The
   *   default authority is taken from the NameNode {@link URI} the current process is co-located with. The default path
   *   is "/user/[current-user]/".
   * </p>
   *
   * @param  configKey       The URI of the config key that needs to be accessed.
   *
   * @return a {@link SimpleHDFSConfigStore} configured with the the given {@link URI}.
   *
   * @throws ConfigStoreCreationException if the {@link SimpleHDFSConfigStore} could not be created.
   */
  @Override
  public SimpleHDFSConfigStore createConfigStore(URI configKey) throws ConfigStoreCreationException {
    FileSystem fs = createFileSystem(configKey);
    URI physicalStoreRoot = getStoreRoot(fs, configKey);
    URI logicalStoreRoot = URI.create(SIMPLE_HDFS_SCHEME_PREFIX + physicalStoreRoot);
    return new SimpleHDFSConfigStore(fs, physicalStoreRoot, logicalStoreRoot);
  }

  /**
   * Returns the physical scheme this {@link ConfigStoreFactory} is responsible for. To support new HDFS
   * {@link FileSystem} implementations, subclasses should override this method.
   */
  protected String getPhysicalScheme() {
    return HDFS_SCHEME_NAME;
  }

  /**
   * Gets a default root directory if one is not specified. The default root dir is {@code /user/[current-user]/}.
   */
  protected Path getDefaultRootDir() throws IOException {
    return new Path("jobs", UserGroupInformation.getCurrentUser().getUserName());
  }

  /**
   * Gets a default authority if one is not specified. The default authority is the authority of the {@link FileSystem}
   * the current process is running on. For example, when running on a HDFS node, the authority will taken from the
   * NameNode {@link URI}.
   */
  private String getDefaultAuthority() throws IOException {
    return FileSystem.get(new Configuration()).getUri().getAuthority();
  }

  /**
   * Creates a {@link FileSystem} given a user specified configKey.
   */
  private FileSystem createFileSystem(URI configKey) throws ConfigStoreCreationException {
    try {
      return FileSystem.get(createHDFSURI(configKey), new Configuration());
    } catch (IOException | URISyntaxException e) {
      throw new ConfigStoreCreationException(configKey, e);
    }
  }

  /**
   * Creates a HDFS {@link URI} given a user specified configKey. If the given configKey does not have an authority,
   * a default one is used instead, provided by the {@link #getDefaultAuthority()} method.
   */
  private URI createHDFSURI(URI configKey) throws URISyntaxException, IOException {
    // Validate the scheme
    String configKeyScheme = configKey.getScheme();
    if (!configKeyScheme.startsWith(SIMPLE_HDFS_SCHEME_PREFIX)) {
      throw new IllegalArgumentException(String.format("Scheme for configKey \"%s\" must begin with \"%s\"!", configKey,
          SIMPLE_HDFS_SCHEME_PREFIX));
    }

    if (Strings.isNullOrEmpty(configKey.getAuthority())) {
      return new URI(getPhysicalScheme(), getDefaultAuthority(), "", "", "");
    }
    return new URI(getPhysicalScheme(), configKey.getAuthority(), "", "", "");
  }

  /**
   * This method determines the physical location of the {@link SimpleHDFSConfigStore} root directory on HDFS. It does
   * this by taking the {@link URI} given by the user and back-tracing the path. It checks if each parent directory
   * contains the folder {@link SimpleHDFSConfigStore#CONFIG_STORE_NAME}. It the assumes this {@link Path} is the root
   * directory.
   *
   * <p>
   *   If the given configKey does not have an authority, then this method assumes the given {@link URI#getPath()} does
   *   not contain the dataset root. In which case it uses the {@link #getDefaultRootDir()} as the root directory. If
   *   the default root dir does not contain the {@link SimpleHDFSConfigStore#CONFIG_STORE_NAME} then a
   *   {@link ConfigStoreCreationException} is thrown.
   * </p>
   */
  private URI getStoreRoot(FileSystem fs, URI configKey) throws ConfigStoreCreationException {
    if (Strings.isNullOrEmpty(configKey.getAuthority())) {
      try {
        Path defaultRootDir = getDefaultRootDir();
        Path configStoreDir = new Path(defaultRootDir, SimpleHDFSConfigStore.CONFIG_STORE_NAME);
        if (fs.exists(configStoreDir)) {
          return fs.getUri().resolve(defaultRootDir.toUri());
        } else {
          throw new ConfigStoreCreationException(configKey,
              String.format("Cannot find store root for default path \"%s\"", configStoreDir));
        }
      } catch (IOException e) {
        throw new ConfigStoreCreationException(configKey, e);
      }
    }

    Path path = new Path(configKey.getPath());

    while (path != null) {
      try {
        for (FileStatus fileStatus : fs.listStatus(path)) {
          if (fileStatus.isDir() && fileStatus.getPath().getName().equals(SimpleHDFSConfigStore.CONFIG_STORE_NAME)) {
            return fs.getUri().resolve(fileStatus.getPath().getParent().toUri());
          }
        }
      } catch (IOException e) {
        throw new ConfigStoreCreationException(configKey, e);
      }
      path = path.getParent();
    }
    throw new ConfigStoreCreationException(configKey, "Cannot find the store root!");
  }
}

