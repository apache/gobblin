package gobblin.source.extractor.hadoop;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelper;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.util.ProxiedFileSystemWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public abstract class HadoopFsHelper implements FileBasedHelper {
  private final State state;
  private final Configuration configuration;
  private FileSystem fs;

  protected HadoopFsHelper(State state, Configuration configuration) {
    this.state = state;
    this.configuration = configuration;
  }

  protected State getState() {
    return this.state;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  @Override
  public void connect() throws FileBasedHelperException {
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI);
    try {
      if (uri == null) {
        throw new FileBasedHelperException(ConfigurationKeys.SOURCE_FILEBASED_FS_URI + " has not been configured");
      }
      this.createFileSystem(uri);
    } catch (IOException e) {
      throw new FileBasedHelperException("Cannot connect to given URI " + uri + " due to " + e.getMessage(), e);
    } catch (URISyntaxException e) {
      throw new FileBasedHelperException("Malformed uri " + uri + " due to " + e.getMessage(), e);
    } catch (InterruptedException e) {
      throw new FileBasedHelperException("Interrupted exception is caught when getting the proxy file system", e);
    }
  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
      List<String> results = new ArrayList<>();
      try {
          lsr(new Path(path), results);
      } catch (IOException e) {
          throw new FileBasedHelperException("Cannot do ls on path " + path + " due to " + e.getMessage(), e);
      }
      return results;
  }

  public void lsr(Path p, List<String> results) throws IOException {
      if (!this.fs.getFileStatus(p).isDir()) {
          results.add(p.toString());
      }
      Path qualifiedPath = this.fs.makeQualified(p);
      for (FileStatus status : this.fs.listStatus(p)) {
          if (status.isDir()) {
              // Fix for hadoop issue: https://issues.apache.org/jira/browse/HADOOP-12169
              if (!qualifiedPath.equals(status.getPath())) {
                  lsr(status.getPath(), results);
              }
          } else {
              results.add(status.getPath().toString());
          }
      }
  }

  private void createFileSystem(String uri) throws IOException, InterruptedException, URISyntaxException {
    if (state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system as a proxy user.
      this.fs =
          new ProxiedFileSystemWrapper().getProxiedFileSystem(state, ProxiedFileSystemWrapper.AuthType.TOKEN,
              state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE), uri);

    } else {
      // Initialize file system as the current user.
      this.fs = FileSystem.get(URI.create(uri), this.configuration);
    }
  }
}
