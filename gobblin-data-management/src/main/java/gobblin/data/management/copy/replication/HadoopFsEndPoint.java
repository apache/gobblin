package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.FileListUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HadoopFsEndPoint implements EndPoint {
  public static final String HDFS_COLO_KEY = "cluster.colo";
  public static final String HDFS_CLUSTERNAME_KEY = "cluster.name";
  public static final String HDFS_FILESYSTEM_URI_KEY = "cluster.FsURI";
  public static final String HDFS_PATH_KEY = "path";
  
  public static final String METADATA = "_metadata";
  public static final String LATEST_TIMESTAMP = "latestTimestamp";
  
  @Getter
  private final String colo;

  private final String clustername;

  private final URI fsURI;

  private final Path path;
  
  private final boolean isSource;
  
  private final String endPointName;
  
  public HadoopFsEndPoint(Config config, boolean isSource, String endPointName) {
    Preconditions.checkArgument(config.hasPath(HDFS_COLO_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_CLUSTERNAME_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_PATH_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_FILESYSTEM_URI_KEY));

    this.colo = config.getString(HDFS_COLO_KEY);
    this.clustername = config.getString(HDFS_CLUSTERNAME_KEY);
    this.path = new Path(config.getString(HDFS_PATH_KEY));
    this.isSource = isSource;
    this.endPointName = endPointName;
    try {
      this.fsURI = new URI(config.getString(HDFS_FILESYSTEM_URI_KEY));
    } catch (URISyntaxException e) {
      throw new RuntimeException("can not build URI based on " + config.getString(HDFS_FILESYSTEM_URI_KEY));
    }
  }

  @Override
  public boolean isSource() {
    return this.isSource;
  }

  @Override
  public String getEndPointName() {
    return this.clustername;
  }

  private LongWatermark getWatermarkForSource(){
    LongWatermark result = new LongWatermark(-1);
    try {
      FileSystem fs = FileSystem.get(this.fsURI, new Configuration());
      List<FileStatus> allFileStatus = FileListUtils.listFilesRecursively(fs, this.path);
      for(FileStatus f: allFileStatus){
        if(f.getModificationTime()>result.getValue()){
          result = new LongWatermark(f.getModificationTime());
        }
      }
      
      return result;
    } catch (IOException e) {
      log.error("Error while retrieve the watermark for " + this);
      return result;
    }
  }
  
  private LongWatermark getWatermarkForReplica(){
    LongWatermark result = new LongWatermark(-1);
    try {
      Path metaData = new Path(this.path, HadoopFsEndPoint.METADATA);
      FileSystem fs = FileSystem.get(this.fsURI, new Configuration());
      if(fs.exists(metaData)){
        FSDataInputStream fin = fs.open(metaData);
        InputStreamReader reader = new InputStreamReader(fin);
        Config c = ConfigFactory.parseReader(reader);
        result = new LongWatermark(c.getLong(HadoopFsEndPoint.LATEST_TIMESTAMP));
      }
      
      // for replica, can not use the file timestamp as that is different with original source timestamp
      return result;
    } catch (IOException e) {
      log.error("Error while retrieve the watermark for " + this);
      return result;
    }
  }
  @Override
  public LongWatermark getWatermark() {
    if(this.isSource){
      return this.getWatermarkForSource();
    }
    else{
      return this.getWatermarkForReplica();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("colo", this.colo).add("name", this.clustername)
        .add("FilesystemURI", this.fsURI).add("rootPath", this.path).toString();

  }
}
