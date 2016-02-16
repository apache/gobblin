package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSimpleSource;

public class KafkaSimpleJsonSource extends KafkaSimpleSource {

    @Override
    public Extractor<String, byte[]> getExtractor(WorkUnitState state) throws IOException {
        return new KafkaSimpleJsonExtractor(state);
    }
}
