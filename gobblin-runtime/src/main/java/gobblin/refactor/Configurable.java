package gobblin.refactor;

import com.typesafe.config.Config;
import java.util.Properties;


public interface Configurable {
  Config getConfig();

  Properties getConfigAsProperties();
}
