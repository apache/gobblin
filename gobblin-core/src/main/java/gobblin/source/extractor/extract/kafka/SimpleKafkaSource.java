package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.util.reflection.GobblinConstructorUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A {@link KafkaSource} to use with arbitrary {@link KafkaExtractor}. Specify the extractor to use with key
 * {@link #EXTRACTOR_TYPE}.
 */
public class SimpleKafkaSource<S, D> extends KafkaSource<S, D> {

  public static final String EXTRACTOR_TYPE = "gobblin.source.kafka.extractorType";

  @AllArgsConstructor
  @Getter
  public enum ExtractorType {
    DESERIALIZER(KafkaDeserializerExtractor.class), AVRO_FIXED_SCHEMA(FixedSchemaKafkaAvroExtractor.class);

    private final Class<? extends KafkaExtractor> klazz;
  }

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    Preconditions.checkArgument(state.contains(EXTRACTOR_TYPE), "Missing key " + EXTRACTOR_TYPE);

    try {
      Class<? extends KafkaExtractor> klazz = ExtractorType.valueOf(state.getProp(EXTRACTOR_TYPE).toUpperCase()).getKlazz();

      return GobblinConstructorUtils.invokeLongestConstructor(klazz, state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
