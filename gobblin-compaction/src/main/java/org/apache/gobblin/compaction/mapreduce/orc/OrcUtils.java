package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;


public class OrcUtils {
  // For Util class to prevent initialization
  private OrcUtils() {

  }

  public static TypeDescription getTypeDescriptionFromFile(Configuration conf, Path orcFilePath) throws IOException {
    return OrcFile.createReader(orcFilePath, new OrcFile.ReaderOptions(conf)).getSchema();
  }

  public static RecordReader getRecordReaderFromFile(Configuration conf, Path orcFilePath) throws IOException {
    return OrcFile.createReader(orcFilePath, new OrcFile.ReaderOptions(conf)).rows();
  }

  // TODO: No newest logic here.
  public static TypeDescription getNewestSchemaFromSource(Job job, FileSystem fs) throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(FileListUtils.listFilesRecursively(fs, sourceDir));
    }

    return getTypeDescriptionFromFile(job.getConfiguration(), files.get(0).getPath());
  }
}
