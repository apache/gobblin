package gobblin.config.regressiontest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientUtils;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.common.impl.ConfigStoreBackedTopology;
import gobblin.config.common.impl.ConfigStoreBackedValueInspector;
import gobblin.config.common.impl.InMemoryTopology;
import gobblin.config.common.impl.InMemoryValueInspector;
import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.hdfs.SimpleHDFSConfigStore;
import gobblin.config.store.hdfs.SimpleHDFSConfigStoreFactory;


/**
 * This class is the Azkaban job which trigger the regression test for a configuration store
 * @author mitu
 *
 */
public class RegressionTest extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(RegressionTest.class);

  public static final String PHYSICAL_CONFIG_STORE_URI_KEY = "physical.config.store.uri.key";
  public static final String LOGICAL_CONFIG_STORE_URI_KEY = "logical.config.store.uri.key";
  private final String physicalConfigStorePath;
  private final String logicalConfigStorePath;
  private SimpleHDFSConfigStore configStore;
  private String configStoreVersion;
  private ExpectedResultBuilder expectedResults;
  private InMemoryTopology inMemoryTopology;
  private InMemoryValueInspector inMemoryValueWithStrongRef;
  private InMemoryValueInspector inMemoryValueWithWeakRef;
  private ConfigClient configClient;

  public RegressionTest(String jobId, Properties props) {
    super(jobId, LOG);
    this.physicalConfigStorePath = props.getProperty(PHYSICAL_CONFIG_STORE_URI_KEY);
    this.logicalConfigStorePath = props.getProperty(LOGICAL_CONFIG_STORE_URI_KEY);
  }

  @Override
  public void run() throws Exception {
    URI physicalURI = new URI(this.physicalConfigStorePath);
    URI logicalURI = new URI(this.logicalConfigStorePath);
    SimpleHDFSConfigStoreFactory storeFactory = new SimpleHDFSConfigStoreFactory();
    this.configStore = storeFactory.createConfigStore(logicalURI);
    this.configStoreVersion = this.configStore.getCurrentVersion();

    LOG.info("Config Store URI is " + this.configStore.getStoreURI());
    LOG.info("Config Store version is " + this.configStoreVersion);

    this.expectedResults = new ExpectedResultBuilder(physicalURI);

    validateRawConfigStore();
    validateThroughInspector_ConfigClient();
  }

  // validate the expected result against the ConfigStore
  private void validateRawConfigStore() {
    LOG.info("Validate all expected results with result from Config Store");
    ConfigKeyPath root = SingleLinkedListConfigKeyPath.ROOT;
    Collection<ConfigKeyPath> currentLevel = new ArrayList<ConfigKeyPath>();
    currentLevel.add(root);
    while (currentLevel.size() > 0) {
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for (ConfigKeyPath path : currentLevel) {
        validateSingleNodeInRawConfigStore(path);
        nextLevel.addAll(this.configStore.getChildren(path, this.configStoreVersion));
      }

      currentLevel = nextLevel;
    }
  }

  private void validateSingleNodeInRawConfigStore(ConfigKeyPath path) {
    LOG.info("Validate against config store with path: " + path.getAbsolutePathString());
    SingleNodeExpectedResultIntf expectSingleResult = this.expectedResults.getSingleNodeExpectedResult(path);

    LOG.info("Validate against config store with path: " + path.getAbsolutePathString() + " for own config");
    ValidationUtils.validateConfigs(this.configStore.getOwnConfig(path, this.configStoreVersion),
        expectSingleResult.getOwnConfig());

    LOG.info("Validate against config store with path: " + path.getAbsolutePathString() + " for own imports");
    ValidationUtils.validateConfigKeyPathsWithOrder(this.configStore.getOwnImports(path, this.configStoreVersion),
        expectSingleResult.getOwnImports());

    LOG.info("Validate against config store with path: " + path.getAbsolutePathString() + " for children");
    ValidationUtils.validateConfigKeyPathsWithOutOrder(this.configStore.getChildren(path, this.configStoreVersion),
        this.expectedResults.getChildren(path));
  }

  /*
   * Validate the expected result through ValueInspector/TopologyInspector and ConfigClient
   */
  private void validateThroughInspector_ConfigClient() throws Exception {
    LOG.info("Validate all expected result with result from ValueInspector/TopologyInspector and ConfigClient");
    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(this.configStore, this.configStoreVersion);
    this.inMemoryTopology = new InMemoryTopology(csTopology);

    ConfigStoreBackedValueInspector rawValueInspector =
        new ConfigStoreBackedValueInspector(this.configStore, this.configStoreVersion, inMemoryTopology);
    this.inMemoryValueWithStrongRef = new InMemoryValueInspector(rawValueInspector, true);
    this.inMemoryValueWithWeakRef = new InMemoryValueInspector(rawValueInspector, false);

    this.configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY);

    ConfigKeyPath root = SingleLinkedListConfigKeyPath.ROOT;
    Collection<ConfigKeyPath> currentLevel = new ArrayList<ConfigKeyPath>();
    currentLevel.add(root);
    // BFS for all the nodes
    while (currentLevel.size() > 0) {
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for (ConfigKeyPath path : currentLevel) {
        SingleNodeExpectedResultIntf expectSingleResult = this.expectedResults.getSingleNodeExpectedResult(path);
        validateSingleNodeInTopology(path, expectSingleResult);
        validateSingleNodeInValueInspector(path, expectSingleResult);
        validateSingleNodeWithConfigClient(path, expectSingleResult);
        nextLevel.addAll(inMemoryTopology.getChildren(path));
      }

      currentLevel = nextLevel;
    }
  }

  private void validateSingleNodeInTopology(ConfigKeyPath path, SingleNodeExpectedResultIntf expectSingleResult)
      throws Exception {
    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString());

    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString() + " for children");
    // validate children
    ValidationUtils.validateConfigKeyPathsWithOutOrder(this.inMemoryTopology.getChildren(path),
        this.expectedResults.getChildren(path));

    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString() + " for own imported by");
    // validate imported By
    ValidationUtils.validateConfigKeyPathsWithOutOrder(this.inMemoryTopology.getImportedBy(path),
        expectSingleResult.getOwnImportedBy());

    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString()
        + " for recursive imported by");
    // validate imported By recursively
    ValidationUtils.validateConfigKeyPathsWithOutOrder(this.inMemoryTopology.getImportedByRecursively(path),
        expectSingleResult.getResolvedImportedBy());

    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString() + " for own imports");
    // validate imports
    ValidationUtils.validateConfigKeyPathsWithOrder(this.inMemoryTopology.getOwnImports(path),
        expectSingleResult.getOwnImports());

    LOG.info("Validate against InMemoryTopology with path: " + path.getAbsolutePathString() + " for recursive imports");
    // validate imports recursively
    ValidationUtils.validateConfigKeyPathsWithOrder(this.inMemoryTopology.getImportsRecursively(path),
        expectSingleResult.getResolvedImports());
  }

  private void validateSingleNodeInValueInspector(ConfigKeyPath path, SingleNodeExpectedResultIntf expectSingleResult)
      throws Exception {
    // check value inspector
    LOG.info("Validate against StrongRef value inpsector with path: " + path.getAbsolutePathString()
        + " for own config");
    ValidationUtils.validateConfigs(this.inMemoryValueWithStrongRef.getOwnConfig(path),
        expectSingleResult.getOwnConfig());

    LOG.info("Validate against StrongRef value inpsector with path: " + path.getAbsolutePathString()
        + " for resolved config");
    ValidationUtils.validateConfigs(this.inMemoryValueWithStrongRef.getResolvedConfig(path),
        expectSingleResult.getResolvedConfig());

    LOG.info("Validate against WeakRef value inpsector with path: " + path.getAbsolutePathString() + " for own config");
    ValidationUtils
        .validateConfigs(this.inMemoryValueWithWeakRef.getOwnConfig(path), expectSingleResult.getOwnConfig());

    LOG.info("Validate against WeakRef value inpsector with path: " + path.getAbsolutePathString()
        + " for resolved config");
    ValidationUtils.validateConfigs(this.inMemoryValueWithWeakRef.getResolvedConfig(path),
        expectSingleResult.getResolvedConfig());
  }

  private void validateSingleNodeWithConfigClient(ConfigKeyPath path, SingleNodeExpectedResultIntf expectSingleResult)
      throws Exception {
    // validate through config client
    URI uri = ConfigClientUtils.buildUriInClientFormat(path, this.configStore, true);
    LOG.info("Validate against config client with uri: " + uri + " for resolved config");
    ValidationUtils.validateConfigs(this.configClient.getConfig(uri), expectSingleResult.getResolvedConfig());

    LOG.info("Validate against config client with uri: " + uri + " for own imports");
    Collection<URI> uris = this.configClient.getImports(uri, false);
    List<ConfigKeyPath> configKeyPathList = this.getConfigKeyPathList(uris);
    ValidationUtils.validateConfigKeyPathsWithOrder(configKeyPathList, expectSingleResult.getOwnImports());

    LOG.info("Validate against config client with uri: " + uri + " for resolved imports");
    uris = this.configClient.getImports(uri, true);
    configKeyPathList = this.getConfigKeyPathList(uris);
    ValidationUtils.validateConfigKeyPathsWithOrder(configKeyPathList, expectSingleResult.getResolvedImports());

    LOG.info("Validate against config client with uri: " + uri + " for own imported by");
    uris = this.configClient.getImportedBy(uri, false);
    configKeyPathList = this.getConfigKeyPathList(uris);
    ValidationUtils.validateConfigKeyPathsWithOutOrder(configKeyPathList, expectSingleResult.getOwnImportedBy());

    LOG.info("Validate against config client with uri: " + uri + " for resolved imported by");
    uris = this.configClient.getImportedBy(uri, true);
    configKeyPathList = this.getConfigKeyPathList(uris);
    ValidationUtils.validateConfigKeyPathsWithOutOrder(configKeyPathList, expectSingleResult.getResolvedImportedBy());
  }

  private List<ConfigKeyPath> getConfigKeyPathList(Collection<URI> uris) {
    List<ConfigKeyPath> result = new ArrayList<>();
    if (result != null) {
      for (URI u : uris) {
        result.add(ConfigClientUtils.buildConfigKeyPath(u, this.configStore));
      }
    }

    return result;
  }
}
