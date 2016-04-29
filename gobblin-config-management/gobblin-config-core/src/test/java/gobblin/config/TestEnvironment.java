package gobblin.config;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import gobblin.config.common.impl.TestConfigStoreValueInspector;
import gobblin.config.store.hdfs.SimpleHdfsConfigStoreTest;


@Test
public class TestEnvironment {

  @BeforeSuite
  public void setup() {
    System.setProperty(SimpleHdfsConfigStoreTest.TAG_NAME_SYS_PROP_KEY, SimpleHdfsConfigStoreTest.TAG_NAME_SYS_PROP_VALUE);
    System.setProperty(TestConfigStoreValueInspector.VALUE_INSPECTOR_SYS_PROP_KEY, TestConfigStoreValueInspector.VALUE_INSPECTOR_SYS_PROP_VALUE);
  }
}
