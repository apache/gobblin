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

package org.apache.gobblin.temporal.dynscale;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import lombok.Data;


public interface ProfileOverlay {

  Config applyOverlay(Config config);

  ProfileOverlay over(ProfileOverlay other);


  @Data
  class KVPair {
    private final String key;
    private final String value;
  }


  @Data
  class Adding implements ProfileOverlay {
    private final List<KVPair> additionPairs;

    @Override
    public Config applyOverlay(Config config) {
      return additionPairs.stream().sequential().reduce(config,
          (currConfig, additionPair) ->
              currConfig.withValue(additionPair.getKey(), ConfigValueFactory.fromAnyRef(additionPair.getValue())),
          (configA, configB) ->
              configB.withFallback(configA)
      );
    }

    @Override
    public ProfileOverlay over(ProfileOverlay other) {
      if (other instanceof Adding) {
        Map<String, String> base = ((Adding) other).getAdditionPairs().stream().collect(Collectors.toMap(KVPair::getKey, KVPair::getValue));
        additionPairs.stream().forEach(additionPair ->
            base.put(additionPair.getKey(), additionPair.getValue()));
        return new Adding(base.entrySet().stream().map(entry -> new KVPair(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
      } else if (other instanceof Removing) {
        return Combo.normalize(this, (Removing) other);
      } else if (other instanceof Combo) {
        Combo otherCombo = (Combo) other;
        return Combo.normalize((Adding) this.over(otherCombo.getAdding()), otherCombo.getRemoving());
      } else {
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }
  }


  @Data
  class Removing implements ProfileOverlay {
    private final List<String> removalKeys;

    @Override
    public Config applyOverlay(Config config) {
      return removalKeys.stream().sequential().reduce(config,
          (currConfig, removalKey) ->
              currConfig.withoutPath(removalKey),
          (configA, configB) ->
              configA.withFallback(configB)
      );
    }

    @Override
    public ProfileOverlay over(ProfileOverlay other) {
      if (other instanceof Adding) {
        return Combo.normalize((Adding) other, this);
      } else if (other instanceof Removing) {
        Set<String> otherKeys = new HashSet<String>(((Removing) other).getRemovalKeys());
        otherKeys.addAll(removalKeys);
        return new Removing(new ArrayList<>(otherKeys));
      } else if (other instanceof Combo) {
        Combo otherCombo = (Combo) other;
        return Combo.normalize(otherCombo.getAdding(), (Removing) this.over(otherCombo.getRemoving()));
      } else {
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }
  }


  @Data
  class Combo implements ProfileOverlay {
    private final Adding adding;
    private final Removing removing;

    // merely restrict access modifier from `public` to `protected`, as not meant to be instantiated outside this enclosing interface
    private Combo(Adding adding, Removing removing) {
      this.adding = adding;
      this.removing = removing;
    }

    protected static Combo normalize(Adding toAdd, Removing toRemove) {
      // pre-remove any in `toAdd` that are also in `toRemove`... yet still maintain them in `toRemove`, in case the eventual `Config` "basis" also has any
      Set<String> removeKeysLookup = toRemove.getRemovalKeys().stream().collect(Collectors.toSet());
      List<KVPair> unmatchedAdditionPairs = toAdd.getAdditionPairs().stream().sequential().filter(additionPair ->
        !removeKeysLookup.contains(additionPair.getKey())
      ).collect(Collectors.toList());
      return new Combo(new Adding(unmatchedAdditionPairs), new Removing(new ArrayList<>(removeKeysLookup)));
    }

    @Override
    public Config applyOverlay(Config config) {
      return adding.applyOverlay(removing.applyOverlay(config));
    }

    @Override
    public ProfileOverlay over(ProfileOverlay other) {
      if (other instanceof Adding) {
        return Combo.normalize((Adding) this.adding.over((Adding) other), this.removing);
      } else if (other instanceof Removing) {
        return Combo.normalize(this.adding, (Removing) this.removing.over((Removing) other));
      } else if (other instanceof Combo) {
        Combo otherCombo = (Combo) other;
        return Combo.normalize((Adding) this.adding.over(otherCombo.getAdding()), (Removing) this.removing.over(otherCombo.getRemoving()));
      } else {
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }
  }
}
