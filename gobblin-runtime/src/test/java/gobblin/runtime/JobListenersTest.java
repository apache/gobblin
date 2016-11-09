package gobblin.runtime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import gobblin.runtime.listeners.CloseableJobListener;
import gobblin.runtime.listeners.JobListener;
import gobblin.runtime.listeners.JobListeners;


@Test(groups = {"gobblin.runtime"})
public class JobListenersTest {

  @Test
  public void testParallelJobListener()
      throws Exception {
      JobContext jobContext = mock(JobContext.class);
    JobListener mockJobListener1 = mock(JobListener.class);
    JobListener mockJobListener2 = mock(JobListener.class);

    CloseableJobListener closeableJobListener =
        JobListeners.parallelJobListener(Lists.newArrayList(mockJobListener1, mockJobListener2));
    closeableJobListener.onJobCompletion(jobContext);
    closeableJobListener.onJobCancellation(jobContext);
    closeableJobListener.close();

    verify(mockJobListener1, times(1)).onJobCompletion(jobContext);
    verify(mockJobListener1, times(1)).onJobCancellation(jobContext);

    verify(mockJobListener2, times(1)).onJobCompletion(jobContext);
    verify(mockJobListener2, times(1)).onJobCancellation(jobContext);
  }
}
