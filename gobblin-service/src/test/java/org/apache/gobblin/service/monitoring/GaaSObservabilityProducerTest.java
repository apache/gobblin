package org.apache.gobblin.service.monitoring;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.InMemoryMultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.service.ExecutionStatus;


public class GaaSObservabilityProducerTest {

  private MultiContextIssueRepository issueRepository = new InMemoryMultiContextIssueRepository();
  Queue<GaaSObservabilityEventExperimental> emittedEvents = new LinkedList<>();

  @Test
  public void testCreateGaaSObservabilityEvent() throws Exception {
    String flowGroup = "testFlowGroup1";
    String flowName = "testFlowName1";
    String jobName = String.format("%s_%s_%s", flowGroup, flowName, "testJobName1");
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    GaaSObservabilityEventProducer producer = new MockGaaSObservabilityProducer(new State(), this.issueRepository);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.METADATA_MESSAGE, "hostName");
    gteEventMetadata.put(TimingEvent.METADATA_START_TIME, "1");
    gteEventMetadata.put(TimingEvent.METADATA_END_TIME, "100");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());

    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);
    producer.emitObservabilityEvent(new State(jobStatusProps));

    Assert.assertEquals(emittedEvents.size(), 1);
    GaaSObservabilityEventExperimental event = emittedEvents.poll();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getJobName(), jobName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getJobStatus(), JobStatus.SUCCEEDED);
    Assert.assertEquals(event.getExecutorUrl(), "hostName");
    Assert.assertEquals(event.getIssues().size(), 1);
  }

  private Issue createTestIssue(String summary, String code, IssueSeverity severity) {
    return Issue.builder().summary(summary).code(code).time(ZonedDateTime.now()).severity(severity).build();
  }


  public class MockGaaSObservabilityProducer extends GaaSObservabilityEventProducer {
    public MockGaaSObservabilityProducer(State state, MultiContextIssueRepository issueRepository) {
      super(state, issueRepository);
    }
    // Send the events to the class test queue, so tests should not run concurrently
    @Override
    protected void sendUnderlyingEvent(GaaSObservabilityEventExperimental event) {
      emittedEvents.add(event);
    }
  }
}
