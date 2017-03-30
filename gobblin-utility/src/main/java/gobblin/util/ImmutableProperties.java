package gobblin.util;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import lombok.experimental.Delegate;


/**
 * Created by ydai on 3/30/17.
 */

public class ImmutableProperties extends Properties {
  @Delegate
  private final Map<Object, Object> props;

  public ImmutableProperties(Properties props) {
    this.props = Collections.unmodifiableMap(props);
  }
}
