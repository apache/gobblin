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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;


/** Alt. forms of profile overlay to evolve one profile {@link Config} into another.  Two overlays may be combined hierarchically into a new overlay. */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class") // to handle impls (`MINIMAL..`, as all defs below)
public interface ProfileOverlay {

  /** @return a new, evolved {@link Config}, by application of this overlay */
  Config applyOverlay(Config config);

  /** @return a new overlay, by combining this overlay *over* another */
  ProfileOverlay over(ProfileOverlay other);

  /** @return a new overlay that would change nothing when used in a {@link ProfileDerivation} (beyond introducing a distinct name) */
  static ProfileOverlay unchanged() {
    return new Adding();
  }


  /** A key-value pair/duple */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  @RequiredArgsConstructor
  class KVPair {
    @NonNull private String key;
    @NonNull private String value;
  }


  /** An overlay to evolve any profile by adding key-value pairs */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @RequiredArgsConstructor  // explicit, due to other ctors
  class Adding implements ProfileOverlay {
    @NonNull private List<KVPair> additionPairs;

    // IMPORTANT: for jackson (de)serialization
    public Adding() {
      this(new ArrayList<>());
    }

    /** variadic, for convenience */
    public Adding(KVPair... kvPairs) {
      this(Arrays.asList(kvPairs));
    }

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
      } else {  // should NEVER happen!
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }
  }


  /** An overlay to evolve any profile by removing named keys */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  @RequiredArgsConstructor  // explicit, due to second, variadic ctor
  class Removing implements ProfileOverlay {
    @NonNull private List<String> removalKeys;

    /** variadic, for convenience */
    public Removing(String... keys) {
      this(Arrays.asList(keys));
    }

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
      } else {  // should NEVER happen!
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }
  }


  /** An overlay to evolve any profile by adding key-value pairs while also removing named keys */
  @Data
  @Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
  @NoArgsConstructor // IMPORTANT: for jackson (de)serialization
  class Combo implements ProfileOverlay {
    @NonNull private Adding adding;
    @NonNull private Removing removing;

    /** restricted-access ctor: instead use {@link Combo#normalize(Adding, Removing)} */
    private Combo(Adding adding, Removing removing) {
      this.adding = adding;
      this.removing = removing;
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
      } else {  // should NEVER happen!
        throw new IllegalArgumentException("unknown derived class of type '" + other.getClass().getName() + "': " + other);
      }
    }

    /** @return a `Combo` overlay, by combining an `Adding` overlay with a `Removing` overlay */
    protected static Combo normalize(Adding toAdd, Removing toRemove) {
      // pre-remove any in `toAdd` that are also in `toRemove`... yet still maintain all in `toRemove`, in case also in the eventual `Config` "basis"
      Set<String> removeKeysLookup = toRemove.getRemovalKeys().stream().collect(Collectors.toSet());
      List<KVPair> unmatchedAdditionPairs = toAdd.getAdditionPairs().stream().sequential().filter(additionPair ->
          !removeKeysLookup.contains(additionPair.getKey())
      ).collect(Collectors.toList());
      return new Combo(new Adding(unmatchedAdditionPairs), new Removing(new ArrayList<>(removeKeysLookup)));
    }
  }
}
