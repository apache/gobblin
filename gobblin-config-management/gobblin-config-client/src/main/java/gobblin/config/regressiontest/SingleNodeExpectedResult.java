package gobblin.config.regressiontest;

import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.client.ConfigClientUtils;
import gobblin.config.store.api.ConfigKeyPath;


/**
 * Used to parse {@link java.io.Reader} which build from the json file to generate expected output
 * 
 * 
 * @author mitu
 *
 */
public class SingleNodeExpectedResult implements SingleNodeExpectedResultIntf {

  private final ExpectedResult expectedResult;

  public SingleNodeExpectedResult(Reader reader) {
    Gson gson = new GsonBuilder().create();
    expectedResult = gson.fromJson(reader, ExpectedResult.class);
  }

  @Override
  public Config getOwnConfig() {
    return ConfigFactory.parseMap(this.expectedResult.OWN_CONFIG);
  }

  @Override
  public Config getResolvedConfig() {
    return ConfigFactory.parseMap(this.expectedResult.RESOLVED_CONFIG);
  }

  @Override
  public List<ConfigKeyPath> getOwnImports() {
    return ConfigClientUtils.getConfigKeyPath(this.expectedResult.OWN_IMPORTS);
  }

  @Override
  public List<ConfigKeyPath> getResolvedImports() {
    return ConfigClientUtils.getConfigKeyPath(this.expectedResult.RESOLVED_IMPORTS);
  }

  @Override
  public Collection<ConfigKeyPath> getOwnImportedBy() {
    return ConfigClientUtils.getConfigKeyPath(this.expectedResult.OWN_IMPORTEDBY);
  }

  @Override
  public Collection<ConfigKeyPath> getResolvedImportedBy() {
    return ConfigClientUtils.getConfigKeyPath(this.expectedResult.RESOLVED_IMPORTEDBY);
  }

  // the java class corresponding to the json format
  class ExpectedResult {
    private List<String> OWN_IMPORTS;
    private List<String> RESOLVED_IMPORTS;

    private List<String> OWN_IMPORTEDBY;
    private List<String> RESOLVED_IMPORTEDBY;

    private Map<String, String> OWN_CONFIG;
    private Map<String, String> RESOLVED_CONFIG;
  }
}
