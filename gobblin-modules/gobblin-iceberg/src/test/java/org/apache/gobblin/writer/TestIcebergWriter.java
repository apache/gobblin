package org.apache.gobblin.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.serde.HiveSerDeConverter;
import org.apache.gobblin.source.extractor.hadoop.OldApiWritableFileExtractor;
import org.apache.gobblin.source.extractor.hadoop.OldApiWritableFileSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.TaskWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class TestIcebergWriter {

    private Table table;

    public void testWrite() {
        Properties properties = new Properties();
        SourceState sourceState = new SourceState(new State(properties), ImmutableList.<WorkUnitState> of());
        OldApiWritableFileSource source = new OldApiWritableFileSource();
        List<WorkUnit> workUnits = source.getWorkunits(sourceState);
        Closer closer = Closer.create();
        IcebergWriter icebergWriter = null;

        WorkUnitState wus = new WorkUnitState(workUnits.get(0));
        wus.addAll(sourceState);

        try {
            OldApiWritableFileExtractor extractor = closer.register((OldApiWritableFileExtractor) source.getExtractor(wus));
            icebergWriter = closer.register((IcebergWriter)createIcebergWriter().build());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private IcebergDataWriterBuilder createIcebergWriter() throws IOException {
        Properties properties = new Properties();
        SourceState sourceState = new SourceState(new State(properties), ImmutableList.<WorkUnitState> of());
        sourceState.setProp("avro.schema" , IcebergTestUtil.generateAvroSchema());
        IcebergDataWriterBuilder icebergDataWriterBuilder = (IcebergDataWriterBuilder)
            new IcebergDataWriterBuilder().withBranches(1)
            .withWriterId("0").writeTo(Destination.of(Destination.DestinationType.HDFS, sourceState))
            .withAttemptId("0-0")
            .writeInFormat(WriterOutputFormat.ORC).build();

        return icebergDataWriterBuilder;
    }
}
