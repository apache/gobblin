package gobblin.runtime.listeners;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.runtime.JobContext;
import gobblin.util.ExecutorsUtils;


/**
 * Static utility methods pertaining to {@link JobListener}s.
 *
 * @see JobListener
 */
public class JobListeners {

  /**
   * Chains a given {@link List} of {@link JobListener}s into a single {@link JobListener}. The specified {@link JobListener}s
   * will all be executed in parallel.
   *
   * @param jobListeners is a {@link List} of {@link JobListener}s that need to be executed
   *
   * @return a {@link CloseableJobListener}, which is similar to {@link JobListener}, except
   * {@link CloseableJobListener#close()} will block until all {@link JobListener}s have finished their executions.
   */
  public static CloseableJobListener parallelJobListener(List<JobListener> jobListeners) {
    Iterables.removeIf(jobListeners, Predicates.isNull());
    return new ParallelJobListener(jobListeners);
  }

  /**
   * Implementation of {@link CloseableJobListener} that executes a given {@link List} of {@link JobListener}s in parallel.
   */
  private static final class ParallelJobListener implements CloseableJobListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelJobListener.class);

    private final List<JobListener> jobListeners;
    private final ExecutorService executor;
    private final CompletionService completionService;

    public ParallelJobListener(List<JobListener> jobListeners) {
      this.jobListeners = jobListeners;
      this.executor = Executors.newCachedThreadPool(
          ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ParallelJobListener")));
      this.completionService = new ExecutorCompletionService(this.executor);
    }

    @Override
    public void onJobPrepare(final JobContext jobContext) {
      for (final JobListener jobListener : jobListeners) {
        this.completionService.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            jobListener.onJobPrepare(jobContext);
            return null;
          }
        });
      }
    }

    @Override
    public void onJobStart(final JobContext jobContext) {
      for (final JobListener jobListener : jobListeners) {
        this.completionService.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            jobListener.onJobStart(jobContext);
            return null;
          }
        });
      }
    }

    @Override
    public void onJobCompletion(final JobContext jobContext) {
      for (final JobListener jobListener : jobListeners) {
        this.completionService.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            jobListener.onJobCompletion(jobContext);
            return null;
          }
        });
      }
    }

    @Override
    public void onJobCancellation(final JobContext jobContext) {
      for (final JobListener jobListener : jobListeners) {
        this.completionService.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            jobListener.onJobCancellation(jobContext);
            return null;
          }
        });
      }
    }

    @Override
    public void onJobFailure(final JobContext jobContext) {
      for (final JobListener jobListener : jobListeners) {
        this.completionService.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            jobListener.onJobFailure(jobContext);
            return null;
          }
        });
      }
    }

    @Override
    public void close()
        throws IOException {
      try {
        for (int i = 0; i < this.jobListeners.size(); i++) {
          this.completionService.take().get();
        }
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      } catch (ExecutionException ee) {
        throw new IOException(ee);
      } finally {
        ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(LOGGER));
      }
    }
  }
}
