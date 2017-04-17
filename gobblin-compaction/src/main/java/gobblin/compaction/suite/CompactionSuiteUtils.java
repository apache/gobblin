package gobblin.compaction.suite;

import com.google.common.collect.ImmutableList;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ClassAliasResolver;
import gobblin.util.reflection.GobblinConstructorUtils;

import java.lang.reflect.InvocationTargetException;

/**
 * A utility class for {@link CompactionSuite}
 */
public class CompactionSuiteUtils {

  /**
   * Return an {@link CompactionSuiteFactory} based on the configuration
   * @return A concrete suite factory instance. By default {@link CompactionAvroSuiteFactory} is used.
   */
  public static CompactionSuiteFactory getCompactionSuiteFactory (State state) {
    try {
      String factoryName = state.getProp(ConfigurationKeys.COMPACTION_SUITE_FACTORY, ConfigurationKeys.DEFAULT_COMPACTION_SUITE_FACTORY);

      ClassAliasResolver<CompactionSuiteFactory> conditionClassAliasResolver = new ClassAliasResolver<>(CompactionSuiteFactory.class);
      CompactionSuiteFactory factory = conditionClassAliasResolver.resolveClass(factoryName).newInstance();
      return factory;
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
