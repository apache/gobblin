package org.apache.gobblin.service.modules.flowgraph.datanodes.iceberg;

import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog;
import org.apache.gobblin.service.modules.dataset.IcebergDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.datanodes.hive.HiveDataNode;

@Alpha
@EqualsAndHashCode(callSuper = true)
public class IcebergDataNode extends HiveDataNode {
  public static final String PLATFORM = "iceberg";
  /**
   * Constructor. A IcebergDataNode must have hive.metastore.uri property specified to get {@link IcebergHiveCatalog} information
   * @param nodeProps
   */
  public IcebergDataNode(Config nodeProps)
      throws DataNodeCreationException {
    super(nodeProps);
  }
  @Override
  public String getDefaultDatasetDescriptorClass() {
    return IcebergDatasetDescriptor.class.getCanonicalName();
  }

  @Override
  public String getDefaultDatasetDescriptorPlatform() {
    return PLATFORM;
  }

}
