package gobblin.config.regressiontest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * This class is used as the place holder for the node which do NOT have the expected result specified.
 * 
 * @author mitu
 *
 */
public class EmptySingleNodeExpectedResult implements SingleNodeExpectedResultIntf {

  @Override
  public Config getOwnConfig() {
    return ConfigFactory.empty();
  }

  @Override
  public Config getResolvedConfig() {
    return ConfigFactory.empty();
  }

  @Override
  public List<ConfigKeyPath> getOwnImports() {
    return Collections.emptyList();
  }

  @Override
  public List<ConfigKeyPath> getResolvedImports() {
    return Collections.emptyList();
  }

  @Override
  public Collection<ConfigKeyPath> getOwnImportedBy() {
    return Collections.emptyList();
  }

  @Override
  public Collection<ConfigKeyPath> getResolvedImportedBy() {
    return Collections.emptyList();
  }

}
