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

package org.apache.gobblin.runtime.troubleshooter;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.runtime.util.GsonUtils;


/**
 * The builder builds builds a specific {@link GobblinTrackingEvent} whose metadata has
 * {@value GobblinEventBuilder#EVENT_TYPE} to be {@value #ISSUE_EVENT_TYPE}
 *
 * <p>
 * Note: A {@link IssueEventBuilder} instance is not reusable
 */
public class IssueEventBuilder extends GobblinEventBuilder {
  public static final String JOB_ISSUE = "JobIssue";

  private static final String ISSUE_EVENT_TYPE = "IssueEvent";
  private static final String METADATA_ISSUE = "issue";

  @Getter
  @Setter
  private Issue issue;

  public IssueEventBuilder(String name) {
    this(name, NAMESPACE);
  }

  public IssueEventBuilder(String name, String namespace) {
    super(name, namespace);
    metadata.put(EVENT_TYPE, ISSUE_EVENT_TYPE);
  }

  public static boolean isIssueEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(ISSUE_EVENT_TYPE);
  }

  /**
   * Create a {@link IssueEventBuilder} from a {@link GobblinTrackingEvent}.
   * Will return null if the event is not of the correct type.
   */
  public static IssueEventBuilder fromEvent(GobblinTrackingEvent event) {
    if (!isIssueEvent(event)) {
      return null;
    }
    Map<String, String> metadata = event.getMetadata();
    IssueEventBuilder issueEventBuilder = new IssueEventBuilder(event.getName(), event.getNamespace());

    metadata.forEach((key, value) -> {
      if (METADATA_ISSUE.equals(key)) {
        issueEventBuilder.issue = GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(value, Issue.class);
      } else {
        issueEventBuilder.addMetadata(key, value);
      }
    });
    return issueEventBuilder;
  }

  public static Issue getIssueFromEvent(GobblinTrackingEvent event) {
    String serializedIssue = event.getMetadata().get(METADATA_ISSUE);
    return GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(serializedIssue, Issue.class);
  }

  public GobblinTrackingEvent build() {
    if (this.issue == null) {
      throw new IllegalStateException("Issue must be set");
    }

    String serializedIssue = GsonUtils.GSON_WITH_DATE_HANDLING.toJson(issue);
    addMetadata(METADATA_ISSUE, serializedIssue);

    return super.build();
  }
}
