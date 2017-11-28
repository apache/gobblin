package org.apache.gobblin.dataset;

import org.apache.gobblin.configuration.State;


public class NoopDatasetResolver implements DatasetResolver {
  public static final NoopDatasetResolver INSTANCE = new NoopDatasetResolver();

  private NoopDatasetResolver() {}

  @Override
  public DatasetDescriptor resolve(DatasetDescriptor raw, State state) {
    return raw;
  }
}
