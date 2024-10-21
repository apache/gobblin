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

package org.apache.gobblin.temporal.dynamic;

import java.util.Optional;
import java.util.function.Function;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.Getter;


@Data
public class ProfileDerivation {
  public static class UnknownBasisException extends Exception {
    @Getter
    private final String name;
    public UnknownBasisException(String basisName) {
      super("named '" + WorkforceProfiles.renderName(basisName) + "'");
      this.name = basisName;
    }
  }

  private final String basisProfileName;
  private final ProfileOverlay overlay;

  public Config formulateConfig(Function<String, Optional<WorkerProfile>> basisResolver) throws UnknownBasisException {
    Optional<WorkerProfile> optProfile = basisResolver.apply(basisProfileName);
    if (!optProfile.isPresent()) {
      throw new UnknownBasisException(basisProfileName);
    } else {
      return overlay.applyOverlay(optProfile.get().getConfig());
    }
  }

  public String renderName() {
    return WorkforceProfiles.renderName(this.basisProfileName);
  }
}
