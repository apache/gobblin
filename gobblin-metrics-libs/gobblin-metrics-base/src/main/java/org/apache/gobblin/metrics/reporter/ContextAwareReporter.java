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

package org.apache.gobblin.metrics.reporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.codahale.metrics.Reporter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.InnerMetricContext;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.context.ReportableContext;
import org.apache.gobblin.metrics.context.filter.ContextFilter;
import org.apache.gobblin.metrics.context.filter.ContextFilterFactory;
import org.apache.gobblin.metrics.notification.MetricContextCleanupNotification;
import org.apache.gobblin.metrics.notification.NewMetricContextNotification;
import org.apache.gobblin.metrics.notification.Notification;


/**
 * Base {@link Reporter} for gobblin metrics. Automatically handles {@link MetricContext} selection,
 * {@link org.apache.gobblin.metrics.Metric} filtering, and changes to reporting on {@link MetricContext} life cycle.
 *
 * <p>
 *   The lifecycle of a {@link ContextAwareReporter} fully managed by the {@link RootMetricContext} is:
 *   {@code construct -> start -> stop -> close}. However, {@link ContextAwareReporter}s created manually by the user
 *   can have a different life cycle (for example  multiple calls to start / stop).
 * </p>
 */
@Slf4j
public class ContextAwareReporter implements Reporter, Closeable {

  private boolean started;
  private final UUID notificationTargetUUID;
  private final Set<InnerMetricContext> contextsToReport;
  private final ContextFilter contextFilter;

  protected final String name;
  protected final Config config;

  public ContextAwareReporter(String name, Config config) {
    this.name = name;
    this.config = config;
    this.started = false;
    RootMetricContext.get().addNewReporter(this);
    this.notificationTargetUUID = RootMetricContext.get().addNotificationTarget(new Function<Notification, Void>() {
      @Nullable @Override public Void apply(Notification input) {
        notificationCallback(input);
        return null;
      }
    });
    this.contextFilter = ContextFilterFactory.createContextFilter(config);
    this.contextsToReport = Sets.newConcurrentHashSet();
    for (MetricContext context : this.contextFilter.getMatchingContexts()) {
      this.contextsToReport.add(context.getInnerMetricContext());
    }
  }

  public boolean isStarted() {
    return this.started;
  }

  /**
   * Starts the {@link ContextAwareReporter}. If the {@link ContextAwareReporter} has been started
   * (and not stopped since), this is a no-op.
   */
  public final void start() {
    if (this.started) {
      log.warn(String.format("Reporter %s has already been started.", this.name));
      return;
    }
    try {
      startImpl();
      this.started = true;
    } catch (Exception exception) {
      log.warn(String.format("Reporter %s did not start correctly.", this.name), exception);
    }
  }

  /**
   * Actual logic for starting the {@link ContextAwareReporter}. This is a separate method from {@link #start()} to
   * allow {@link ContextAwareReporter} to handle the started / non-started state.
   */
  protected void startImpl() {
  }

  /**
   * Stops the {@link ContextAwareReporter}. If the {@link ContextAwareReporter} has not been started, or if it has been
   * stopped already, and not started since, this is a no-op.
   */
  public final void stop() {
    if (!this.started) {
      log.warn(String.format("Reporter %s has already been stopped.", this.name));
      return;
    }
    try {
      stopImpl();
      this.started = false;
    } catch (Exception exception) {
      log.warn(String.format("Reporter %s did not stop correctly.", this.name), exception);
    }
  }

  /**
   * Actual logic for stopping the {@link ContextAwareReporter}. This is a separate method from {@link #stop()} to
   * allow {@link ContextAwareReporter} to handle the started / non-started state.
   */
  protected void stopImpl() {
  }

  /**
   * Removes {@link ContextAwareReporter} records from the {@link RootMetricContext}.
   * This method should be considered irreversible and destructive to the {@link ContextAwareReporter}.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    RootMetricContext.get().removeNotificationTarget(this.notificationTargetUUID);
    RootMetricContext.get().removeReporter(this);
  }

  /**
   * Callback used to receive notifications from the {@link RootMetricContext}.
   */
  private void notificationCallback(Notification notification) {
    if (notification instanceof MetricContextCleanupNotification) {
      removedMetricContext(((MetricContextCleanupNotification) notification).getMetricContext());
    }
    if (notification instanceof NewMetricContextNotification) {
      newMetricContext(((NewMetricContextNotification) notification).getMetricContext());
    }
  }

  /**
   * Called when any {@link MetricContext} is removed from the tree.
   *
   * @param context {@link InnerMetricContext} backing the removed {@link MetricContext}.
   */
  protected void removedMetricContext(InnerMetricContext context) {
    this.contextsToReport.remove(context);
    if (context.getParent().isPresent() && this.contextFilter.shouldReplaceByParent(context)) {
      this.contextsToReport.add(context.getParent().get().getInnerMetricContext());
    }
  }

  /**
   * Called whenever a new {@link MetricContext} is added to the tree.
   *
   * @param context new {@link MetricContext} added.
   */
  protected void newMetricContext(MetricContext context) {
    if (this.contextFilter.matches(context)) {
      this.contextsToReport.add(context.getInnerMetricContext());
    }
  }

  /**
   * Whether a {@link InnerMetricContext} should be reported. Called when a {@link MetricContext} has been removed and
   * just before the corresponding {@link InnerMetricContext} is removed.
   */
  protected boolean shouldReportInnerMetricContext(InnerMetricContext context) {
    return this.contextsToReport.contains(context);
  }

  /**
   * @return an {@link Iterable} of all {@link MetricContext}s to report.
   */
  protected Iterable<ReportableContext> getMetricContextsToReport() {
    return ImmutableSet.<ReportableContext>copyOf(this.contextsToReport);
  }
}
