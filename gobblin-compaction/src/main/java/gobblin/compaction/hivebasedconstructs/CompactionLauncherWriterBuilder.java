package gobblin.compaction.hivebasedconstructs;

import java.io.IOException;
import org.apache.avro.Schema;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;


/**
 * {@link DataWriterBuilder} for {@link CompactionLauncherWriter}
 */
public class CompactionLauncherWriterBuilder extends DataWriterBuilder<Schema, MRCompactionEntity> {
  @Override
  public DataWriter<MRCompactionEntity> build() throws IOException {
    return new CompactionLauncherWriter();
  }
}
