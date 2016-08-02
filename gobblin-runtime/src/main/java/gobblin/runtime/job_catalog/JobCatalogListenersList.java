package gobblin.runtime.job_catalog;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

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
    Preconditions.checkNotNull(newListener);
    _listeners.add(newListener);
  }

  public synchronized void removeListener(JobCatalogListener oldListener) {
    Preconditions.checkNotNull(oldListener);
    _listeners.remove(oldListener);
  }

  @Override
  public synchronized void onAddJob(JobSpec addedJob) {
    Preconditions.checkNotNull(addedJob);
    callbackAllListeners(new AddJobCallback(addedJob));
  }

  @Override
  public synchronized void onDeleteJob(JobSpec deletedJob) {
    Preconditions.checkNotNull(deletedJob);
    callbackAllListeners(new DeleteJobCallback(deletedJob));
  }

  @Override
  public synchronized void onUpdateJob(JobSpec originalJob, JobSpec updatedJob) {
    Preconditions.checkNotNull(originalJob);
    Preconditions.checkNotNull(updatedJob);
    callbackAllListeners(new UpdateJobCallback(originalJob, updatedJob));
  }

  public void callbackAllListeners(Callback callback) {
    for (JobCatalogListener listener: _listeners) {
      callbackOneListener(callback, listener);
    }
  }

  public void callbackOneListener(Callback callback, JobCatalogListener listener) {
    String callbackMsg = null;
    if (_debugLogEnabled) {
      callbackMsg = "callback " + callback + " on " + listener;
      _log.debug("Started: " + callbackMsg);
    }
    try {
      callback.invoke(listener);
    }
    catch (RuntimeException e) {
      if (null == callbackMsg) {
        callbackMsg = "callback " + callback + " on " + listener;
      }
      _log.error("FAILED: " + callbackMsg + " ; error : " + e);
    }
    if (_debugLogEnabled) {
      _log.debug("Finished: " + callbackMsg);
    }
  }

  @RequiredArgsConstructor
  public static abstract class Callback {
    final protected String methodName;
    final protected JobSpec param1;
    final protected JobSpec param2;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(this.methodName);
      sb.append('(');
      sb.append('[');
      jobSpecToParamString(param1, sb);
      if (null != param2) {
        sb.append(']');
        sb.append(',');
        jobSpecToParamString(param2, sb);
      }
      sb.append(']');
      sb.append(')');
      return sb.toString();
    }

    public abstract void invoke(JobCatalogListener listener);

    private static StringBuilder jobSpecToParamString(JobSpec js, StringBuilder sb) {
      return sb.append(js.getUri().toString()).append(',').append(js.getVersion());
    }
  }

  public static class AddJobCallback extends Callback {
    public AddJobCallback(JobSpec addedJob) {
      super("onAddJob", addedJob, null);
    }

    @Override
    public void invoke(JobCatalogListener listener) {
      listener.onAddJob(this.param1);
    }
  }

  public static class DeleteJobCallback extends Callback {
    public DeleteJobCallback(JobSpec deletedJob) {
      super("onDeleteJob", deletedJob, null);
    }

    @Override
    public void invoke(JobCatalogListener listener) {
      listener.onDeleteJob(this.param1);
    }
  }

  public static class UpdateJobCallback extends Callback {
    public UpdateJobCallback(JobSpec originalJob, JobSpec updatedJob) {
      super("onUpdateJob", originalJob, updatedJob);
    }

    @Override
    public void invoke(JobCatalogListener listener) {
      listener.onUpdateJob(this.param1, this.param2);
    }
  }


}
