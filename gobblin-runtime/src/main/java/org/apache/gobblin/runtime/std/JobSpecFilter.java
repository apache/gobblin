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
package org.apache.gobblin.runtime.std;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.gobblin.runtime.api.JobSpec;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * A helper class to create JobSpec predicates.
 */
@AllArgsConstructor
public class JobSpecFilter implements Predicate<JobSpec> {
  private final Optional<Predicate<URI>> uriPredicate;
  private final Optional<Predicate<String>> versionPredicate;

  @Override
  public boolean apply(JobSpec input) {
    Preconditions.checkNotNull(input);

    boolean res = true;
    if (this.uriPredicate.isPresent()) {
      res &= this.uriPredicate.get().apply(input.getUri());
    }
    if (res && this.versionPredicate.isPresent()) {
      res &= this.versionPredicate.get().apply(input.getVersion());
    }
    return res;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static JobSpecFilter eqJobSpecURI(URI jobSpecURI) {
    return builder().eqURI(jobSpecURI).build();
  }

  public static JobSpecFilter eqJobSpecURI(String jobSpecURI) {
    return builder().eqURI(jobSpecURI).build();
  }

  @Getter
  @Setter
  public static class Builder {
    private Predicate<URI> uriPredicate;
    private Predicate<String> versionPredicate;

    public Builder eqURI(URI uri) {
      this.uriPredicate = Predicates.equalTo(uri);
      return this;
    }

    public Builder eqURI(String uri) {
      try {
        this.uriPredicate = Predicates.equalTo(new URI(uri));
      } catch (URISyntaxException e) {
        throw new RuntimeException("invalid URI: " + uri, e);
      }
      return this;
    }

    public Builder eqVersion(String version) {
      this.versionPredicate = Predicates.equalTo(version);
      return this;
    }

    public JobSpecFilter build() {
      return new JobSpecFilter(Optional.fromNullable(this.uriPredicate),
             Optional.fromNullable(this.versionPredicate));
    }
  }

}
