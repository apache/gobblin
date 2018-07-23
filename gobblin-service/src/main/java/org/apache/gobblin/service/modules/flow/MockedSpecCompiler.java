package org.apache.gobblin.service.modules.flow;

import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.util.ConfigUtils;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * This mocked SpecCompiler class creates 3 dummy job specs to emulate multi hop flow spec compiler.
 * It uses {@link InMemorySpecExecutor} for these dummy specs.
 */
public class MockedSpecCompiler extends IdentityFlowToJobSpecCompiler {

  private static final int NUMBER_OF_JOBS = 3;

  public MockedSpecCompiler(Config config) {
    super(config);
  }

  @Override
  public Map<Spec, SpecExecutor> compileFlow(Spec spec) {
    Map<Spec, SpecExecutor> specExecutorMap = Maps.newLinkedHashMap();

    SpecExecutor specExecutor = new InMemorySpecExecutor(ConfigFactory.empty());
    long flowExecutionId = System.currentTimeMillis();

    int i = 0;
    while(i++ < NUMBER_OF_JOBS) {
      String specUri = "/foo/bar/spec/" + i;
      Properties properties = new Properties();
      properties.put(ConfigurationKeys.FLOW_NAME_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY));
      properties.put(ConfigurationKeys.FLOW_GROUP_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_GROUP_KEY));
      properties.put(ConfigurationKeys.JOB_NAME_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY) + "_" + i);
      properties.put(ConfigurationKeys.JOB_GROUP_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_GROUP_KEY) + "_" + i);
      properties.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
      Spec jobSpec = JobSpec.builder(specUri)
          .withConfig(ConfigUtils.propertiesToConfig(properties))
          .withVersion("1")
          .withDescription("Spec Description")
          .build();
      specExecutorMap.put(jobSpec, specExecutor);
    }

    return specExecutorMap;
  }
}
