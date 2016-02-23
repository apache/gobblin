package gobblin.config.regressiontest;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.hdfs.SimpleHDFSConfigStore;
import gobblin.config.store.hdfs.SimpleHDFSConfigStoreFactory;

import azkaban.jobExecutor.AbstractJob;

public class RegressionTest extends AbstractJob  {

  private static final Logger LOG = Logger.getLogger(RegressionTest.class);

  private final Properties properties;
  
  public static final String PHYSICAL_CONFIG_STORE_URI_KEY = "physical.config.store.uri.key";
  public static final String LOGICAL_CONFIG_STORE_URI_KEY = "logical.config.store.uri.key";
  private final String physicalConfigStorePath;
  private final String logicalConfigStorePath;
  private SimpleHDFSConfigStore configStore;
  private String configStoreVersion;
  private ExpectedResultBuilder expectedResults;

  public RegressionTest(String jobId, Properties props) {
    super(jobId, LOG);
    this.properties = new Properties();
    this.properties.putAll(props);
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
    
    LOG.info("AAA version is " + this.configStoreVersion);
    
    this.expectedResults = new ExpectedResultBuilder(physicalURI);
    
    validateRawConfigStore();
  }

  private void validateRawConfigStore(){
    ConfigKeyPath root = SingleLinkedListConfigKeyPath.ROOT;
    Collection<ConfigKeyPath> currentLevel = new ArrayList<ConfigKeyPath>();
    currentLevel.add(root);
    while(currentLevel.size()>0){
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for(ConfigKeyPath path: currentLevel){
        validateSingleNodeInRawConfigStore(path);
        nextLevel.addAll(this.configStore.getChildren(path, this.configStoreVersion));
      }
      
      currentLevel = nextLevel;
    }
  }
  
  private void validateSingleNodeInRawConfigStore(ConfigKeyPath path){
    LOG.info("AAA path is " + path.getAbsolutePathString());
    SingleNodeExpectedResultIntf expectSingleResult = this.expectedResults.getSingleNodeExpectedResult(path);
    
    ValidationUtils.validateConfigs(
        this.configStore.getOwnConfig(path, this.configStoreVersion), 
        expectSingleResult.getOwnConfig());
    
    ValidationUtils.validateConfigKeyPathsWithOrder(
        this.configStore.getOwnImports(path, this.configStoreVersion), 
        expectSingleResult.getOwnImports());
    
    ValidationUtils.validateConfigKeyPathsWithOutOrder(
        this.configStore.getChildren(path, this.configStoreVersion), 
        this.expectedResults.getChildren(path));
  }
}
