package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;


/**
 * A {@link KafkaAvroDeserializer} implementation which supports schema evolution of Avro records
 * by taking the latest schema from Confluent's Schema Registry as a reader schema.
 *
 * <p>
 *   Slightly modified implementation, borrowed from {@link io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer} 
 * </p>
 * 
 * @author Lorand Bendig
 *
 */
public class KafkaAvroWithReaderSchemaDeserializer extends KafkaAvroDeserializer {

  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<String, Schema>();
  private Schema latestSchema;

  public KafkaAvroWithReaderSchemaDeserializer() {
    super();
  }

  public KafkaAvroWithReaderSchemaDeserializer(SchemaRegistryClient client) {
    super(client);
  }

  public KafkaAvroWithReaderSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    super(client, props);
  }
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.latestSchema = (Schema) configs.get(KafkaDeserializerExtractor.KAFKA_TOPIC_LATEST_SCHEMA);
    super.configure(configs, isKey);
  }

  /**
   * {@inheritDoc}
   * @see io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer#deserialize(byte[])
   */
  protected Object deserialize(byte[] payload) throws SerializationException {
    return deserialize(null, null, payload);
  }
  
  /**
   * @see io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
   */
  protected Object deserialize(String topic, Boolean isKey, byte[] payload) throws SerializationException {
    if (payload == null) {
      return null;
    }

    int id = -1;
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      id = buffer.getInt();
      Schema schema = schemaRegistry.getByID(id);
      int length = buffer.limit() - 1 - idSize;
      final Object result;
      if (schema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        result = bytes;
      } else {
        int start = buffer.position() + buffer.arrayOffset();
        // The latest schema in the registry is used as reader schema
        DatumReader<GenericRecord> reader = getDatumReader(schema, latestSchema);
        Object object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

        if (schema.getType().equals(Schema.Type.STRING)) {
          object = object.toString(); // Utf8 -> String
        }
        result = object;
      }
      return result;
    } catch (IOException | RuntimeException e) {
      // Avro deserialization may throw AvroRuntimeException, NullPointerException, etc
      throw new SerializationException("Error deserializing Avro message for id " + id, e);
    } catch (RestClientException e) {
      throw new SerializationException("Error retrieving Avro schema for id " + id, e);
    }
  }

  /**
   * @see io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer#getByteBuffer(byte[])
   */
  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
  }

  /**
   * @see io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
   */
  private DatumReader<GenericRecord> getDatumReader(Schema writerSchema, Schema readerSchema) {
    if (useSpecificAvroReader) {
      if (readerSchema == null) {
        readerSchema = getReaderSchema(writerSchema);
      }
      return new SpecificDatumReader<GenericRecord>(writerSchema, readerSchema);
    } else {
      if (readerSchema == null) {
        return new GenericDatumReader<GenericRecord>(writerSchema);
      }
      return new GenericDatumReader<GenericRecord>(writerSchema, readerSchema);
    }
  }

  /**
   * @see io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
   */
  @SuppressWarnings("unchecked")
  private Schema getReaderSchema(Schema writerSchema) {
    Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());
    if (readerSchema == null) {
      Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
      if (readerClass != null) {
        try {
          readerSchema = readerClass.newInstance().getSchema();
        } catch (InstantiationException e) {
          throw new SerializationException(writerSchema.getFullName() + " specified by the "
              + "writers schema could not be instantiated to find the readers schema.");
        } catch (IllegalAccessException e) {
          throw new SerializationException(writerSchema.getFullName() + " specified by the "
              + "writers schema is not allowed to be instantiated to find the readers schema.");
        }
        readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
      } else {
        throw new SerializationException("Could not find class " + writerSchema.getFullName()
            + " specified in writer's schema whilst finding reader's schema for a SpecificRecord.");
      }
    }
    return readerSchema;
  }
}
