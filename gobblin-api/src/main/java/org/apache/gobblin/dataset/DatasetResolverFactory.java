package org.apache.gobblin.dataset;

import org.apache.gobblin.configuration.State;


public interface DatasetResolverFactory {
  String NAMESPACE = "DatasetResolverFactory";
  String CLASS = NAMESPACE + "." + "class";

  DatasetResolver createResolver(State state);
}
