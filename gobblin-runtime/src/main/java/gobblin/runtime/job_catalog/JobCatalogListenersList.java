/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.runtime.job_catalog;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobCatalogListenersContainer;
import gobblin.runtime.api.JobSpec;
import gobblin.util.callbacks.CallbacksDispatcher;


/** A helper class to manage a list of {@link JobCatalogListener}s for a
 * {@link JobCatalog}. It will dispatch the callbacks to each listener sequentially.*/
public class JobCatalogListenersList implements JobCatalogListener, JobCatalogListenersContainer, Closeable {
  private final CallbacksDispatcher<JobCatalogListener> _disp;

  public JobCatalogListenersList() {
    this(Optional.<Logger>absent());
  }

  public JobCatalogListenersList(Optional<Logger> log) {
    _disp = new CallbacksDispatcher<JobCatalogListener>(Optional.<ExecutorService>absent(), log);
  }

  public Logger getLog() {
    return _disp.getLog();
  }

  public synchronized List<JobCatalogListener> getListeners() {
    return _disp.getListeners();
  }

  @Override
  public synchronized void addListener(JobCatalogListener newListener) {
    _disp.addListener(newListener);
  }

  @Override
  public synchronized void removeListener(JobCatalogListener oldListener) {
    _disp.removeListener(oldListener);
  }

  @Override
  public synchronized void onAddJob(JobSpec addedJob) {
    Preconditions.checkNotNull(addedJob);
    try {
      _disp.execCallbacks(new AddJobCallback(addedJob));
    } catch (InterruptedException e) {
      getLog().warn("onAddJob interrupted.");
    }
  }

  @Override
  public synchronized void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
    Preconditions.checkNotNull(deletedJobURI);

    try {
      _disp.execCallbacks(new DeleteJobCallback(deletedJobURI, deletedJobVersion));
    } catch (InterruptedException e) {
      getLog().warn("onDeleteJob interrupted.");
    }
  }

  @Override
  public synchronized void onUpdateJob(JobSpec updatedJob) {
    Preconditions.checkNotNull(updatedJob);
    try {
      _disp.execCallbacks(new UpdateJobCallback(updatedJob));
    } catch (InterruptedException e) {
      getLog().warn("onUpdateJob interrupted.");
    }
  }

  @Override
  public void close()
      throws IOException {
    _disp.close();
  }

  public void callbackOneListener(Function<JobCatalogListener, Void> callback,
                                  JobCatalogListener listener) {
    try {
      _disp.execCallbacks(callback, listener);
    } catch (InterruptedException e) {
      getLog().warn("callback interrupted: "+ callback);
    }
  }

  @Override
  public void registerWeakJobCatalogListener(JobCatalogListener jobListener) {
    _disp.addWeakListener(jobListener);
  }

}
