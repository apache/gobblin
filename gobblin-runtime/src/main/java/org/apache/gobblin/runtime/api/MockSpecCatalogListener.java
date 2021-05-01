package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;


public class MockSpecCatalogListener implements SpecCatalogListener {
  public static final String UNCOMPILABLE_FLOW = "uncompilableFlow";

  public AddSpecResponse onAddSpec(Spec addedSpec) {
    String flowName = (String) ((FlowSpec) addedSpec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY);
    if (flowName.equals(UNCOMPILABLE_FLOW)) {
      throw new RuntimeException("Could not compile flow");
    }
    return new AddSpecResponse(null);
  }


  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion, Properties headers) {
    return;
  }

  public void onUpdateSpec(Spec updatedSpec) {
    return;
  }
}
