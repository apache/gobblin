package org.apache.gobblin.temporal.ddm.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;


@UtilityClass
public class TemporalWorkFlowUtils {

  public static Map<String, Object> generateGaasSearchAttributes(@NonNull Properties jobProps) {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(Help.GAAS_FLOW_KEY, String.format("%s.%s", jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY),
        jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));
    attributes.put(Help.USER_TO_PROXY_KEY, jobProps.getProperty(Help.USER_TO_PROXY_KEY));
    return attributes;
  }
}
