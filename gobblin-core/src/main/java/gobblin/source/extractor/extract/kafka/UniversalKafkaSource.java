package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.util.ClassAliasResolver;
import gobblin.util.reflection.GobblinConstructorUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A {@link KafkaSource} to use with arbitrary {@link KafkaExtractor}. Specify the extractor to use with key
 * {@link #EXTRACTOR_TYPE}.
 */
public class UniversalKafkaSource<S, D> extends KafkaSource<S, D> {

  public static final String EXTRACTOR_TYPE = "gobblin.source.kafka.extractorType";

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    Preconditions.checkArgument(state.contains(EXTRACTOR_TYPE), "Missing key " + EXTRACTOR_TYPE);

    try {
      ClassAliasResolver<KafkaExtractor> aliasResolver = new ClassAliasResolver<>(KafkaExtractor.class);
      Class<? extends KafkaExtractor> klazz = aliasResolver.resolveClass(state.getProp(EXTRACTOR_TYPE));

      return GobblinConstructorUtils.invokeLongestConstructor(klazz, state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
