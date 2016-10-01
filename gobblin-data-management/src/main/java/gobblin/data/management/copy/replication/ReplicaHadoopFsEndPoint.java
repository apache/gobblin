package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.extract.LongWatermark;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ReplicaHadoopFsEndPoint implements HadoopFsEndPoint {
  public static final String WATERMARK_FILE = "_metadata";
  public static final String LATEST_TIMESTAMP = "latestTimestamp";

  @Getter
  private final HadoopFsReplicaConfig rc;

  @Getter
  private final String replicaName;

  public ReplicaHadoopFsEndPoint(HadoopFsReplicaConfig rc, String replicaName) {
    Preconditions.checkArgument(!replicaName.equals(ReplicationConfiguration.REPLICATION_SOURCE),
        "replicaName can not be " + ReplicationConfiguration.REPLICATION_SOURCE);
    this.rc = rc;
    this.replicaName = replicaName;
  }

  @Override
  public Watermark getWatermark() {
    LongWatermark result = new LongWatermark(-1);
    try {
      Path metaData = new Path(rc.getPath(), WATERMARK_FILE);
      FileSystem fs = FileSystem.get(rc.getFsURI(), new Configuration());
      if (fs.exists(metaData)) {
        try(FSDataInputStream fin = fs.open(metaData)){
          InputStreamReader reader = new InputStreamReader(fin, Charsets.UTF_8);
          Config c = ConfigFactory.parseReader(reader);
          result = new LongWatermark(c.getLong(LATEST_TIMESTAMP));
        }
      }
      // for replica, can not use the file time stamp as that is different with original source time stamp
      return result;
    } catch (IOException e) {
      log.warn("Can not find " + WATERMARK_FILE + " for replica " + this);
      return result;
    }
  }

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public String getEndPointName() {
    return this.replicaName;
  }
  
  @Override
  public String getClusterName() {
    return this.rc.getClustername();
  }
}
