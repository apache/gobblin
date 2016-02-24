package gobblin.config.regressiontest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;

import com.google.common.base.Preconditions;
import com.google.gson.JsonSyntaxException;


/**
 * This class is used to build all the expected result for all the nodes in the configuration store's version.
 * 
 * The class will do a BFS from the root path and treat all the directories as valid nodes, also will parse the json
 * file with name {@link #EXPECTED_RESULT_FILE} to generate the expected result for that node.
 * 
 * @author mitu
 *
 */
public class ExpectedResultBuilder {

  private static final Logger LOG = Logger.getLogger(ExpectedResultBuilder.class);
  public static final String EXPECTED_RESULT_FILE = "expected.conf";
  private final FileSystem fs;
  private RawNode rootNode;
  private final Map<ConfigKeyPath, SingleNodeExpectedResult> cachedResult = new HashMap<>();

  /**
   * @param rootURI   : the root URI to build the expected result. This URI need to point to the absolute physical URI for the
   * version in the configuration store, for example hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/configStoreRegressionTest/_CONFIG_STORE/v1.0
   * @throws IOException : if can not create the {@link FileSystem}
   */
  public ExpectedResultBuilder(URI rootURI) throws IOException {
    LOG.info("Build expected result for " + rootURI);
    this.fs = FileSystem.get(rootURI, new Configuration());
    Path rootPath = new Path(rootURI);
    Preconditions.checkState(this.fs.exists(rootPath));

    rootNode = new RawNode("");
    FileStatus[] childrenFileStatus = this.fs.listStatus(rootPath);
    buildResultHelper(rootNode, childrenFileStatus, SingleLinkedListConfigKeyPath.ROOT);
  }

  private void buildResultHelper(RawNode rawNode, FileStatus[] childrenFileStatus, ConfigKeyPath configKeyPath)
      throws IOException {

    for (FileStatus f : childrenFileStatus) {
      // new node
      if (f.isDir()) {
        String childName = f.getPath().getName();
        RawNode childRawNode = new RawNode(childName);
        rawNode.addChild(childRawNode);
        ConfigKeyPath childConfigKeyPath = configKeyPath.createChild(childName);
        FileStatus[] nextLevelFileStatus = fs.listStatus(f.getPath());
        this.buildResultHelper(childRawNode, nextLevelFileStatus, childConfigKeyPath);
      }
      // build expected result for current node
      else if (f.getPath().getName().equals(EXPECTED_RESULT_FILE)) {
        try (Reader r = new InputStreamReader(this.fs.open(f.getPath()))) {
          SingleNodeExpectedResult expected = new SingleNodeExpectedResult(r);
          this.cachedResult.put(configKeyPath, expected);
        } catch (JsonSyntaxException jsonException) {
          LOG.error("Caught JsonException while parsing file " + f.getPath());
          throw jsonException;
        }
      }
    }
  }

  /**
   * Used to 
   * @param configKeyPath
   * @return
   */
  public SingleNodeExpectedResultIntf getSingleNodeExpectedResult(ConfigKeyPath configKeyPath) {
    if (this.cachedResult.containsKey(configKeyPath)) {
      return this.cachedResult.get(configKeyPath);
    }

    return new EmptySingleNodeExpectedResult();
  }

  public List<ConfigKeyPath> getChildren(ConfigKeyPath configKeyPath) {
    List<ConfigKeyPath> result = new ArrayList<>();
    if (configKeyPath.isRootPath()) {
      for (RawNode r : this.rootNode.getChildren()) {
        result.add(configKeyPath.createChild(r.getOwnName()));
      }
      return result;
    }

    String absPath = configKeyPath.getAbsolutePathString();
    String[] content = absPath.split("/");

    RawNode rawNode = getRawNode(content, this.rootNode, 0);
    if (rawNode == null) {
      return result;
    }

    for (RawNode r : rawNode.getChildren()) {
      result.add(configKeyPath.createChild(r.getOwnName()));
    }
    return result;
  }

  private RawNode getRawNode(String[] configKeyPath_absPath, RawNode rawNode, int index) {
    if (configKeyPath_absPath[index].equals(rawNode.getOwnName())) {
      if (index == configKeyPath_absPath.length - 1) {
        return rawNode;
      }

      index++;
      RawNode child = rawNode.getChild(configKeyPath_absPath[index]);
      if (child == null) {
        return null;
      }

      return getRawNode(configKeyPath_absPath, child, index);
    }

    return null;
  }
}
