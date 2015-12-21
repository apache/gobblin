package gobblin.applift.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;

/**
 * @author prashant.bhardwaj@applift.com
 *
 */
public class KafkaJsonExtractor extends KafkaExtractor<String, String> {

	private static final Logger log = LoggerFactory.getLogger(KafkaJsonExtractor.class);

	protected final Optional<KafkaSchemaRegistry<String, String>> schemaRegistry;
  protected final String schema;
  
	public KafkaJsonExtractor(WorkUnitState state) {
	    super(state);
	    this.schemaRegistry = state.contains(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS)
	        ? Optional.of(KafkaSchemaRegistry.<String, String> get(state.getProperties()))
	        : Optional.<KafkaSchemaRegistry<String, String>> absent();
	    this.schema = getExtractorSchema();
	  }
	
	/**
   * Get the schema to be used by this extractor.
   */
  protected String getExtractorSchema() {
    return getLatestSchemaByTopic();
  }
  
  private String getLatestSchemaByTopic() {
    Preconditions.checkState(this.schemaRegistry.isPresent());
    try {
      return this.schemaRegistry.get().getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      log.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", this.topicName), e);
      return null;
    }
  }

	@Override
	protected String decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
		return getJsonString(messageAndOffset.message().payload());
	}

	/**
	 * Get the schema (metadata) of the extracted data records.
	 *
	 * @return the Kafka topic being extracted
	 * @throws IOException
	 *           if there is problem getting the schema
	 */
	@Override
	public String getSchema() throws IOException {
		return this.schema;
	}
	
	
	
	/**
	 * Returns Json String from payload byte buffer.
	 * @param buf
	 * @return Json String
	 */
	
	protected static String getJsonString(ByteBuffer buf) {
    byte[] bytes = null;
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    String jsonString = new String(bytes,Charset.forName("UTF-8"));
    return jsonString;
  }
}
