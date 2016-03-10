package gobblin.config.regressiontest;

import java.util.Collection;
import java.util.List;

import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * Indicate the interface for the expected result of a single node in configuration store.
 * The node could be data set or tag.
 * 
 * @author mitu
 *
 */
public interface SingleNodeExpectedResultIntf {

  public Config getOwnConfig();

  public Config getResolvedConfig();

  public List<ConfigKeyPath> getOwnImports();

  public List<ConfigKeyPath> getResolvedImports();

  public Collection<ConfigKeyPath> getOwnImportedBy();

  public Collection<ConfigKeyPath> getResolvedImportedBy();
}
