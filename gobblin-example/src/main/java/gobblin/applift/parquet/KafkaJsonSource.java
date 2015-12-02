package gobblin.applift.parquet;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

/*
 * @author prashant.bhardwaj@applift.com
 * 
 */

public class KafkaJsonSource extends KafkaSource<String, String> {

	@Override
	public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
		return new KafkaJsonExtractor(state);
	}

}
