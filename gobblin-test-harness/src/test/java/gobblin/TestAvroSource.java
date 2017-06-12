package gobblin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;


public class TestAvroSource implements Source<Schema, GenericRecord> {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.addAll(state);
    return Collections.singletonList(workUnit);
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    return new TestAvroExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
