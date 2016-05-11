package gobblin.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

/** A helper class to create {@link Config} objects */
public class ConfigBuilder {
  private final Map<String, Object> _primitiveProps = new HashMap<>();
  private final Optional<String> _originDestription;
  private Config _currentConfig;

  ConfigBuilder(Optional<String> originDescription) {
    _originDestription = originDescription;
    _currentConfig = originDescription.isPresent() ? ConfigFactory.empty(_originDestription.get())
                                                   : ConfigFactory.empty();
  }

  /**
   * Loads properties which have a given name prefix into the config. The following restrictions
   * apply:
   * <ul>
   *    <li>No property can have a name that is equal to the prefix
   *    <li>After removal of the prefix, the remaining property name should start with a letter.
   * </ul>
   *
   * @param  props          the collection from where to load the properties
   * @param  scopePrefix    only properties with this prefix will be considered. The prefix will be
   *                        removed from the names of the keys added to the {@link Config} object.
   *                        The prefix can be an empty string but cannot be null.
   */
  public ConfigBuilder loadProps(Properties props, String scopePrefix) {
    Preconditions.checkNotNull(props);
    Preconditions.checkNotNull(scopePrefix);

    int scopePrefixLen = scopePrefix.length();

    for (Map.Entry<Object, Object> propEntry: props.entrySet()) {
      String propName = propEntry.getKey().toString();
      if (propName.startsWith(scopePrefix)) {
        String scopedName = propName.substring(scopePrefixLen);
        if (scopedName.isEmpty()) {
          throw new RuntimeException("Illegal scoped property:" + propName);
        }
        if (! Character.isAlphabetic(scopedName.charAt(0))) {
          throw new RuntimeException("Scoped name for property " + propName +
                                     " should start with a character: " + scopedName);
        }
        _primitiveProps.put(scopedName, propEntry.getValue());
      }
    }

    return this;
  }

  public ConfigBuilder addPrimitive(String name, Object value) {
    _primitiveProps.put(name, value);
    return this;
  }

  public ConfigBuilder addList(String name, Iterable<? extends Object> values) {
    _currentConfig = _originDestription.isPresent()
        ? _currentConfig.withValue(name, ConfigValueFactory.fromIterable(values, _originDestription.get()))
        : _currentConfig.withValue(name, ConfigValueFactory.fromIterable(values));
    return this;
  }

  public static ConfigBuilder create() {
    return new ConfigBuilder(Optional.<String>absent());
  }

  public static ConfigBuilder create(String originDescription) {
    return new ConfigBuilder(Optional.of(originDescription));
  }

  public Config build() {
    return ConfigFactory.parseMap(_primitiveProps).withFallback(_currentConfig);
  }

}
