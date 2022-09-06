package org.apache.gobblin.data.management.copy.iceberg;

import lombok.Getter;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.partition.FileSet;


@Getter
public abstract class IcebergFileSet extends FileSet<CopyEntity> {

  private final IcebergDataset _icebergDataset;

  public IcebergFileSet(String name, IcebergDataset icebergDataset){
    super(name, icebergDataset);
    this._icebergDataset = icebergDataset;
  }
}
