package gobblin.config.regressiontest;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * Utility class to compare results
 * @author mitu
 *
 */
public class ValidationUtils {
  private static final Logger LOG = Logger.getLogger(ValidationUtils.class);

  /**
   * Compare two {@link Config}s
   * @param config1
   * @param config2
   */
  public static void validateConfigs(Config config1, Config config2) {
    if (config1.entrySet().size() != config2.entrySet().size()) {
      displayConfig(config1, "Config1 ");
      displayConfig(config2, "Config2 ");
      throw new RuntimeException("Config entries size not match");
    }

    for (Map.Entry<String, ConfigValue> entry : config1.entrySet()) {
      if (!entry.getValue().equals(config2.getValue(entry.getKey()))) {
        displayConfig(config1, "Config1 ");
        displayConfig(config2, "Config2 ");
        throw new RuntimeException("Config entry not match for " + entry.getKey());
      }
    }
  }

  /**
   * Compare two {@link List} with exact ordering
   * @param list1
   * @param list2
   */
  public static void validateConfigKeyPathsWithOrder(List<ConfigKeyPath> list1, List<ConfigKeyPath> list2) {
    if (!continueCheck(list1, list2)) {
      return;
    }

    for (int i = 0; i < list1.size(); i++) {
      if (!list1.get(i).equals(list2.get(i))) {
        display(list1, "List1 ");
        display(list2, "List2 ");
        throw new RuntimeException("ConfigKeyPath not match for " + list1.get(i));
      }
    }
  }

  /**
   * Compare two {@link Collection} , ignoring the order
   * @param list1
   * @param list2
   */
  public static void validateConfigKeyPathsWithOutOrder(Collection<ConfigKeyPath> collection1,
      Collection<ConfigKeyPath> collection2) {

    if (!continueCheck(collection1, collection2)) {
      return;
    }

    Set<ConfigKeyPath> set1 = new HashSet<>(collection1);
    Set<ConfigKeyPath> set2 = new HashSet<>(collection2);

    if (set1.size() != set2.size()) {
      display(collection1, "Collection1 ");
      display(collection2, "Collection2 ");
      throw new RuntimeException("Set size not match");
    }

    for (ConfigKeyPath path1 : set1) {
      if (!set2.contains(path1)) {
        display(collection1, "Collection1 ");
        display(collection2, "Collection2 ");
        throw new RuntimeException("ConfigKeyPath not match for " + path1);
      }
    }
  }

  // treat empty collection the same as null
  private static boolean continueCheck(Collection<ConfigKeyPath> collection1, Collection<ConfigKeyPath> collection2) {
    if (collection1 == null) {
      if (collection2 == null || collection2.size() == 0) {
        return false;
      }

      throw new RuntimeException("List size not match");
    }

    if (collection2 == null) {
      if (collection1 == null || collection1.size() == 0) {
        return false;
      }

      throw new RuntimeException("List size not match");
    }

    if (collection1.size() != collection2.size()) {
      throw new RuntimeException("List size not match");
    }
    return true;
  }

  private static void display(Collection<ConfigKeyPath> col, String prefix) {
    if (col == null)
      return;
    for (ConfigKeyPath path : col) {
      LOG.info(prefix + " " + path);
    }
  }

  private static void displayConfig(Config c, String prefix) {
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      LOG.info(prefix + " key:" + entry.getKey() + "-> " + entry.getValue());
    }
  }
}
