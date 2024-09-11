package org.apache.gobblin.temporal.ddm.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;


/**
 * Utility class for handling Temporal workflow-related operations.
 */
@UtilityClass
public class TemporalWorkFlowUtils {

  /**
   * Generates search attributes for a WorkFlow  based on the provided GAAS job properties.
   *
   * @param jobProps the properties of the job, must not be null.
   * @return a map containing the generated search attributes.
   */
  public static Map<String, Object> generateGaasSearchAttributes(@NonNull Properties jobProps) {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(Help.GAAS_FLOW_KEY, String.format("%s.%s", jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY),
        jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));
    attributes.put(Help.USER_TO_PROXY_SEARCH_KEY, jobProps.getProperty(Help.USER_TO_PROXY_KEY));
    return attributes;
  }

  /**
   * Converts search attribute values from a map of lists to a map of objects.
   *
   * @param searchAttributes a map where the keys are attribute names and the values are lists of attribute values.
   *                         Can be null.
   * @return a map where the keys are attribute names and the values are the corresponding attribute values.
   *         If the input map is null, an empty map is returned.
   */
  public static Map<String, Object> convertSearchAttributesValuesFromListToObject(
      Map<String, List<?>> searchAttributes) {
    if (searchAttributes == null) {
      return null;
    }
    Map<String, Object> convertedAttributes = new HashMap<>();

    convertedAttributes.putAll(searchAttributes);

    return convertedAttributes;
  }
}