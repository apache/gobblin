package org.apache.gobblin.dataset;

import com.typesafe.config.Config;


/**
 * Factory to create a {@link DescriptorResolver} instance
 */
public interface DescriptorResolverFactory {
  /**
   * @param config configurations only about {@link DescriptorResolver}
   */
  DescriptorResolver createResolver(Config config);
}
