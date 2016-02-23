package gobblin.config.regressiontest;

import java.util.Collection;
import java.util.List;

import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;

public interface SingleNodeExpectedResultIntf {
  public Config getOwnConfig();
  
  public Config getResolvedConfig();
  
  public List<ConfigKeyPath> getOwnImports();
  
  public List<ConfigKeyPath> getResolvedImports();
  
  public Collection<ConfigKeyPath> getOwnImportedBy();
  
  public Collection<ConfigKeyPath> getResolvedImportedBy();
}
