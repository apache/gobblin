package gobblin.runtime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import com.google.common.collect.Lists;

import org.testng.annotations.Test;


@Test(groups = {"gobblin.runtime"})
public class JobListenersTest {

  @Test
  public void testParallelJobListener()
      throws IOException {
    JobState jobState = mock(JobState.class);
    JobListener mockJobListener1 = mock(JobListener.class);
    JobListener mockJobListener2 = mock(JobListener.class);

    CloseableJobListener closeableJobListener =
        JobListeners.parallelJobListener(Lists.newArrayList(mockJobListener1, mockJobListener2));
    closeableJobListener.onJobCompletion(jobState);
    closeableJobListener.onJobCancellation(jobState);
    closeableJobListener.close();

    verify(mockJobListener1, times(1)).onJobCompletion(jobState);
    verify(mockJobListener1, times(1)).onJobCancellation(jobState);

    verify(mockJobListener2, times(1)).onJobCompletion(jobState);
    verify(mockJobListener2, times(1)).onJobCancellation(jobState);
  }
}
