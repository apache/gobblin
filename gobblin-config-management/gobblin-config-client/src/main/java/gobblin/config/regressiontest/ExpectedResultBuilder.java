package gobblin.config.regressiontest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

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
  private final Map<ConfigKeyPath, SingleNodeExpectedResult> expectedValueCachedResult = new HashMap<>();

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
          this.expectedValueCachedResult.put(configKeyPath, expected);
        } catch (JsonSyntaxException jsonException) {
          LOG.error("Caught JsonException while parsing file " + f.getPath());
          throw jsonException;
        }
      }
    }
  }

  /**
   * Used to get the expected result for the given {@link ConfigKeyPath}
   * 
   * @param configKeyPath : input {@link ConfigKeyPath} for the configuration store
   * @return the specified expected result from parsing {@link #EXPECTED_RESULT_FILE}, 
   * or {@link EmptySingleNodeExpectedResult} if not specified
   */
  public SingleNodeExpectedResultIntf getSingleNodeExpectedResult(ConfigKeyPath configKeyPath) {
    if (this.expectedValueCachedResult.containsKey(configKeyPath)) {
      return this.expectedValueCachedResult.get(configKeyPath);
    }

    return new EmptySingleNodeExpectedResult();
  }

  /**
   * Used to find the children {@link RawNode} based on the input {@link ConfigKeyPath}. This is used 
   * to validate the topology
   * 
   * @param configKeyPath : input {@link ConfigKeyPath}
   * @return the children {@link ConfigKeyPath} based on the {@link RawNode} topology
   */
  public List<ConfigKeyPath> getChildren(ConfigKeyPath configKeyPath) {
    List<ConfigKeyPath> result = new ArrayList<>();
    RawNode rawNode = this.rootNode;

    Stack<String> paths = new Stack<>();
    if (!configKeyPath.isRootPath()) {
      String absPath = configKeyPath.getAbsolutePathString();
      String[] content = absPath.split("/");
      for (int i = content.length - 1; i >= 0; i--) {
        if (!content[i].equals("")) {
          paths.push(content[i]);
        }
      }

      rawNode = getRawNode(paths, this.rootNode);
    }

    if (rawNode == null) {
      return result;
    }

    for (RawNode r : rawNode.getChildren()) {
      result.add(configKeyPath.createChild(r.getOwnName()));
    }
    return result;
  }

  /**
   * Find the corresponding RawNode
   * @param paths       : path starts from currentNode's children
   */
  private RawNode getRawNode(Stack<String> paths, RawNode currentNode) {
    if (paths.isEmpty()) {
      return currentNode;
    }

    String childPath = paths.pop();
    RawNode child = currentNode.getChild(childPath);
    if (child == null) {
      return null;
    }

    return getRawNode(paths, child);
  }
}
