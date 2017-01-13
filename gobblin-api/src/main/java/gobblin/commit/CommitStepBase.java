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

package gobblin.commit;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;


/**
 * A base implementation of {@link CommitStep}.
 *
 * @author Ziyang Liu
 */
@Alpha
public abstract class CommitStepBase implements CommitStep {

  protected final State props;

  protected CommitStepBase(Builder<? extends Builder<?>> builder) {
    Preconditions.checkNotNull(builder.props);
    this.props = builder.props;
  }

  public abstract static class Builder<T extends Builder<?>> {
    private final Optional<CommitSequence.Builder> commitSequenceBuilder;

    protected State props;

    protected Builder() {
      this.commitSequenceBuilder = Optional.<CommitSequence.Builder> absent();
    }

    protected Builder(CommitSequence.Builder commitSequenceBuilder) {
      Preconditions.checkNotNull(commitSequenceBuilder);
      this.commitSequenceBuilder = Optional.of(commitSequenceBuilder);
    }

    @SuppressWarnings("unchecked")
    public T withProps(State props) {
      this.props = new State(props.getProperties());
      return (T) this;
    }

    public CommitSequence.Builder endStep() throws IOException {
      Preconditions.checkState(this.commitSequenceBuilder.isPresent());
      this.commitSequenceBuilder.get().addStep(build());
      return this.commitSequenceBuilder.get();
    }

    public abstract CommitStep build() throws IOException;
  }

}
