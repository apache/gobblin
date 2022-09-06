package org.apache.gobblin.data.management.copy.iceberg;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


public class IcebergTableFileSet extends IcebergFileSet{

  private final IcebergCopyEntityHelper helper;

  public IcebergTableFileSet(String name, IcebergDataset icebergDataset, IcebergCopyEntityHelper helper){
    super(name, icebergDataset);
    this.helper = helper;
  }

  @Override
  protected Collection<CopyEntity> generateCopyEntities() throws IOException {

    String fileSet = this.helper.getIcebergDataset().getInputTableName();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    Map<Path, FileStatus> mapOfPathsToCopy = this.helper.getPath();

    for (CopyableFile.Builder builder : this.helper.getCopyableFilesFromPaths(mapOfPathsToCopy, this.helper.getConfiguration())){
      CopyableFile fileEntity =
          builder.fileSet(fileSet).datasetOutputPath(this.helper.getTargetFs().getUri().getPath()).build();
      fileEntity.setSourceData(this.helper.getSourceDataset());
      fileEntity.setDestinationData(this.helper.getDestinationDataset());
      copyEntities.add(fileEntity);
    }
    return copyEntities;
  }
}
