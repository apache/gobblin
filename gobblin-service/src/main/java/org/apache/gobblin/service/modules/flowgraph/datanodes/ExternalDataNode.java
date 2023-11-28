package org.apache.gobblin.service.modules.flowgraph.datanodes;

import com.typesafe.config.Config;
import org.apache.gobblin.service.modules.dataset.ExternalDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.BaseDataNode;


/**
 * A DataNode for generic ingress/egress data movement outside of HDFS (HTTP or otherwise)
 */
public class ExternalDataNode extends BaseDataNode {
  public static final String EXTERNAL_PLATFORM_NAME = "external";

  public ExternalDataNode(Config nodeProps) throws DataNodeCreationException {
    super(nodeProps);
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return EXTERNAL_PLATFORM_NAME;
  }

  @Override
  public String getDefaultDatasetDescriptorClass() {
    return ExternalDatasetDescriptor.class.getCanonicalName();
  }
}
