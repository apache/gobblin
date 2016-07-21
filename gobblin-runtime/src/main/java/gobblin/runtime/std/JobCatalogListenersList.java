package gobblin.runtime.std;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;

import lombok.RequiredArgsConstructor;

/** A helper class to manage a list of {@link JobCatalogListener}s for a
 * {@link JobCatalog}. It will dispatch the callbacks to each listener sequentially.*/
public class JobCatalogListenersList implements JobCatalogListener {
  private final Logger _log;
  private final boolean _debugLogEnabled;
  private final List<JobCatalogListener> _listeners = new ArrayList<>();

  public JobCatalogListenersList() {
    this(Optional.<Logger>absent());
  }

  public JobCatalogListenersList(Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _debugLogEnabled = _log.isDebugEnabled();
  }

  public synchronized List<JobCatalogListener> getListeners() {
    return new ArrayList<>(_listeners);
  }

  public synchronized void addListener(JobCatalogListener newListener) {
    _listeners.add(newListener);
  }

  public synchronized void removeListener(JobCatalogListener oldListener) {
    _listeners.remove(oldListener);
  }

  @Override
  public synchronized void onAddJob(JobSpec addedJob) {
    for (JobCatalogListener listener: _listeners) {
      safeCall(new AddJobRunnable(listener, addedJob));
    }
  }

  @Override
  public synchronized void onDeleteJob(JobSpec deletedJob) {
    for (JobCatalogListener listener: _listeners) {
      safeCall(new DeleteJobRunnable(listener, deletedJob));
    }
  }

  @Override
  public synchronized void onUpdateJob(JobSpec originalJob, JobSpec updatedJob) {
    for (JobCatalogListener listener: _listeners) {
      safeCall(new UpdateJobRunnable(listener, originalJob, updatedJob));
    }
  }

  private void safeCall(Runnable methodCall) {
    if (_debugLogEnabled) {
      _log.debug("Starting call: " + methodCall);
    }
    try {
      methodCall.run();
    }
    catch (RuntimeException e) {
      _log.error("Call failed: " + methodCall + "; error : " + e, e);
    }
    if (_debugLogEnabled) {
      _log.debug("Finished call: " + methodCall);
    }
  }

  @RequiredArgsConstructor
  static abstract class MethodRunnable implements Runnable {
    final protected String methodName;
    final protected JobCatalogListener listener;

    @Override
    public String toString() {
      return this.methodName + " for " + this.listener;
    }
  }

  static class AddJobRunnable extends MethodRunnable {
    final JobSpec addedJob;

    public AddJobRunnable(JobCatalogListener listener, JobSpec addedJob) {
      super("onAddJob", listener);
      this.addedJob = addedJob;
    }

    @Override
    public void run() {
      this.listener.onAddJob(this.addedJob);
    }
  }

  static class DeleteJobRunnable extends MethodRunnable {
    final JobSpec deletedJob;

    public DeleteJobRunnable(JobCatalogListener listener, JobSpec deletedJob) {
      super("onDeleteJob", listener);
      this.deletedJob = deletedJob;
    }

    @Override
    public void run() {
      this.listener.onDeleteJob(this.deletedJob);
    }
  }

  static class UpdateJobRunnable extends MethodRunnable {
    final JobSpec originalJob;
    final JobSpec updatedJob;

    public UpdateJobRunnable(JobCatalogListener listener, JobSpec originalJob, JobSpec updatedJob) {
      super("onUpdateJob", listener);
      this.originalJob = originalJob;
      this.updatedJob = updatedJob;
    }

    @Override
    public void run() {
      this.listener.onUpdateJob(this.originalJob, this.updatedJob);
    }
  }


}
