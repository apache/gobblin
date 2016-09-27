package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.FileListUtils;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SourceHadoopFsEndPoint implements EndPoint {

  private final HadoopFsReplicaConfig rc;

  public SourceHadoopFsEndPoint(HadoopFsReplicaConfig rc) {
    this.rc = rc;
  }

  @Override
  public Watermark getWatermark() {
    LongWatermark result = new LongWatermark(-1);
    try {
      FileSystem fs = FileSystem.get(rc.getFsURI(), new Configuration());
      List<FileStatus> allFileStatus = FileListUtils.listFilesRecursively(fs, rc.getPath());
      for (FileStatus f : allFileStatus) {
        if (f.getModificationTime() > result.getValue()) {
          result = new LongWatermark(f.getModificationTime());
        }
      }

      return result;
    } catch (IOException e) {
      log.error("Error while retrieve the watermark for " + this);
      return result;
    }
  }

  @Override
  public boolean isSource() {
    return true;
  }

  @Override
  public String getEndPointName() {
    return ReplicationConfiguration.REPLICATION_SOURCE;
  }
}
