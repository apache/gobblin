package gobblin.applift.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;

/**
 * @author prashantbhardwaj
 *
 */
public class KafkaJsonExtractor extends KafkaExtractor<String, String> {

	public KafkaJsonExtractor(WorkUnitState state) {
	    super(state);
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
		return null;
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
