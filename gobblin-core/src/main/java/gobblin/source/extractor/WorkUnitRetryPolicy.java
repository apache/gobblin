/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * An enumeration of supported work unit retry policies.
 *
 * @author ynli
 */
public enum WorkUnitRetryPolicy {
  /**
   * Always retry failed/aborted work units regardless of job commit policies.
   */
  ALWAYS("always"),

  /**
   * Only retry failed/aborted work units when
   * {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} is used.
   * This option is useful for being a global policy for a group of jobs that
   * have different commit policies.
   */
  ON_COMMIT_ON_PARTIAL_SUCCESS("onpartial"),

  /**
   * Only retry failed/aborted work units when
   * {@link JobCommitPolicy#COMMIT_ON_FULL_SUCCESS} is used.
   * This option is useful for being a global policy for a group of jobs that
   * have different commit policies.
   */
  ON_COMMIT_ON_FULL_SUCCESS("onfull"),

  /**
   * Never retry failed/aborted work units.
   */
  NEVER("never");

  private final String name;

  WorkUnitRetryPolicy(String name) {
    this.name = name;
  }

  /**
   * Get a {@link WorkUnitRetryPolicy} of the given name.
   *
   * @param name Work unit retry policy name
   * @return a {@link WorkUnitRetryPolicy} of the given name
   * @throws java.lang.IllegalArgumentException if the name does not represent a
   *         valid work unit retry policy
   */
  public static WorkUnitRetryPolicy forName(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

    if (ALWAYS.name.equalsIgnoreCase(name)) {
      return ALWAYS;
    }

    if (ON_COMMIT_ON_PARTIAL_SUCCESS.name.equalsIgnoreCase(name)) {
      return ON_COMMIT_ON_PARTIAL_SUCCESS;
    }

    if (ON_COMMIT_ON_FULL_SUCCESS.name.equalsIgnoreCase(name)) {
      return ON_COMMIT_ON_FULL_SUCCESS;
    }

    if (NEVER.name.equalsIgnoreCase(name)) {
      return NEVER;
    }

    throw new IllegalArgumentException(String.format("Work unit retry policy with name %s is not supported", name));
  }
}
