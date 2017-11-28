package org.apache.gobblin.dataset;

import org.apache.gobblin.configuration.State;


/**
 * Resolve a raw gobblin dataset to a job specific dataset
 */
public interface DatasetResolver {
  DatasetDescriptor resolve(DatasetDescriptor raw, State state);
}
