package gobblin.azkaban;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * Utility class for collecting metadata specific to a Azkaban runtime environment.
 */
@Slf4j
public class AzkabanTags {

  public static final ImmutableMap<String, String> PROPERTIES_TO_TAGS_MAP = new ImmutableMap.Builder<String, String>()
      .put("azkaban.flow.projectname", "azkabanProjectName")
      .put("azkaban.flow.flowid", "azkabanFlowId")
      .put("azkaban.job.id", "azkabanJobId")
      .put("azkaban.flow.execid", "azkabanExecId").build();

  /**
   * Uses {@link #getAzkabanTags(Configuration)} with default Hadoop {@link Configuration}
   */
  public static Map<String, String> getAzkabanTags() {
    return getAzkabanTags(new Configuration());
  }

  /**
   * Gets all useful Azkaban runtime properties required by metrics as a {@link Map}.
   *
   * @param conf Hadoop Configuration that contains the properties. Keys of {@link #PROPERTIES_TO_TAGS_MAP} lists out
   * all the properties to look for in {@link Configuration}.
   *
   * @return a {@link Map} with keys as property names (name mapping in {@link #PROPERTIES_TO_TAGS_MAP}) and the value
   * of the property from {@link Configuration}
   */
  public static Map<String, String> getAzkabanTags(Configuration conf) {
    Map<String, String> tagMap = Maps.newHashMap();

    for (Map.Entry<String, String> entry : PROPERTIES_TO_TAGS_MAP.entrySet()) {
      if (StringUtils.isNotBlank(conf.get(entry.getKey()))) {
        tagMap.put(entry.getValue(), conf.get(entry.getKey()));
      } else {
        log.warn(String.format("No config value found for config %s. Metrics will not have tag %s", entry.getKey(),
            entry.getValue()));
      }
    }
    return tagMap;
  }
}
