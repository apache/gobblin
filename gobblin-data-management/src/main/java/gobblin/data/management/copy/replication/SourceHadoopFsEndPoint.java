package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Objects;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.FileListUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SourceHadoopFsEndPoint implements HadoopFsEndPoint{

  @Getter
  private final HadoopFsReplicaConfig rc;

  public SourceHadoopFsEndPoint(HadoopFsReplicaConfig rc) {
    this.rc = rc;
  }

  @Override
  public ComparableWatermark getWatermark() {
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

  @Override
  public String getClusterName() {
    return this.rc.getClustername();
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("is source", this.isSource()).add("end point name", this.getEndPointName())
        .add("hadoopfs config", this.rc).toString();
  }
  
  @Override
  public URI getFsURI() {
    return this.rc.getFsURI();
  }
}
