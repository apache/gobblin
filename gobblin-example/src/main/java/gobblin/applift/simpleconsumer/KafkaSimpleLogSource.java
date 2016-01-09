package gobblin.applift.simpleconsumer;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

public class KafkaSimpleLogSource extends KafkaSource<String, String> {

	@Override
	public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
		return new KafkaSimpleLogExtractor(state);
	}

}
