package gobblin.compaction.hivebasedconstructs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import gobblin.compaction.listeners.CompactorListener;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.metrics.Tag;
import gobblin.writer.DataWriter;
import gobblin.compaction.mapreduce.avro.ConfBasedDeltaFieldProvider;


/**
 * {@link DataWriter} that launches an {@link MRCompactor} job given an {@link MRCompactionEntity} specifying config
 * for compaction
 *
 * Delta field is passed using {@link ConfBasedDeltaFieldProvider}
 */
public class CompactionLauncherWriter implements DataWriter<MRCompactionEntity> {
  @Override
  public void write(MRCompactionEntity compactionEntity) throws IOException {
    Preconditions.checkNotNull(compactionEntity);

    List<? extends Tag<?>> list = new ArrayList<>();

    Properties props = new Properties();
    props.putAll(compactionEntity.getProps());
    props.setProperty(ConfBasedDeltaFieldProvider.DELTA_FIELDS_KEY,
        Joiner.on(',').join(compactionEntity.getDeltaList()));
    props.setProperty(MRCompactor.COMPACTION_INPUT_DIR, compactionEntity.getDataFilesPath().toString());

    MRCompactor compactor = new MRCompactor(props, list, Optional.<CompactorListener>absent());
    compactor.compact();
  }

  @Override
  public void commit() throws IOException {}

  @Override
  public void cleanup() throws IOException {}

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
