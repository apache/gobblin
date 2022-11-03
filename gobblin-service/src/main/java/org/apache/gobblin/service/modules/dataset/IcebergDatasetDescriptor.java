package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
public class IcebergDatasetDescriptor extends SqlDatasetDescriptor {
  public IcebergDatasetDescriptor(Config config)
      throws IOException {
    super(config);
  }

  @Override
  protected boolean isPlatformValid() {
    return "iceberg".equalsIgnoreCase(getPlatform());
  }

  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    if (otherPath == null) {
      return false;
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      return false;
    }

    String otherDbName = parts.get(0);
    String otherTableName = parts.get(1);

    return this.databaseName.equals(otherDbName) && this.tableName.equals(otherTableName);
  }
}
