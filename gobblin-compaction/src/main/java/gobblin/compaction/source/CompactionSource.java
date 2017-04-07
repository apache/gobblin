package gobblin.compaction.source;

import com.google.common.collect.Lists;

import gobblin.compaction.dataset.CompactionParser;
import gobblin.compaction.dataset.CompactionPartition;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.compaction.mapreduce.MRCompactionTaskFactory;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * A source class which use {@link CompactionPartitionGlobFinder} to find all the {@link CompactionPartition} units
 * and convert them to available workunits.
 */
public class CompactionSource implements Source<String, String> {


  protected FileSystem getFileSystem(State state)
      throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    return fs;
  }

  public List<WorkUnit> getWorkunits(SourceState state) {
    // TODO: get from and to range
    List<WorkUnit> workUnits = Lists.newArrayList();

    try {
      FileSystem fs = getFileSystem(state);
      CompactionPartitionGlobFinder finder = new CompactionPartitionGlobFinder(fs, state);
      List<CompactionPartition> partitions = finder.findDatasets();

      for (CompactionPartition partition: partitions) {
        CompactionParser parser = partition.getSuite().getParser();
        CompactionVerifier verifier = partition.getSuite().getVerifiers().get(CompactionVerifier.COMPACTION_VERIFIER_DUMMY);
        CompactionParser.CompactionParserResult rst = parser.parse(partition);

        DateTime time = rst.getTime();
        DateTime start = time.minusDays(1);
        DateTime end = time.plusDays(1);
        if (time.isBefore(end) && time.isAfter(start)) {
          if (verifier.verify(partition)) {
            WorkUnit workUnit = createWorkUnit(partition);
            workUnits.add(workUnit);
          }
        }
      }
    } catch (IOException e) {
      return null;
    }

    return workUnits;
  }

  public WorkUnit createWorkUnit (CompactionPartition partition) throws IOException {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, MRCompactionTaskFactory.class);
    partition.save(workUnit);
    return workUnit;
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown(SourceState state) {
    throw new UnsupportedOperationException();
  }
}
