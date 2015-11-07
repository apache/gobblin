package gobblin.config.configstore.impl;

import java.util.Collection;

import gobblin.config.configstore.VersionComparator;
import gobblin.config.utils.VersionUtils;

public class SimpleVersionComparator implements VersionComparator<String>{

  @Override
  public String getCurrentVersion(Collection<String> versions) {
    return VersionUtils.getCurrentVersion(versions);
  }

}
