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

package org.apache.gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.Extract;

import lombok.Getter;
import lombok.Setter;


/**
 * A container for all meta data related to a particular source. This includes all properties
 * defined in job configuration files and all properties from tasks of the previous run.
 *
 * <p>
 *   Properties can be overwritten at runtime and persisted upon job completion. Persisted
 *   properties will be loaded in the next run and made available to use by the
 *   {@link org.apache.gobblin.source.Source}.
 * </p>
 *
 * @author kgoodhop
 */
public class SourceState extends State {

  private static final Set<Extract> EXTRACT_SET = Sets.newConcurrentHashSet();
  private static final DateTimeFormatter DTF =
      DateTimeFormat.forPattern("yyyyMMddHHmmss").withLocale(Locale.US).withZone(DateTimeZone.UTC);

  @Getter
  private final Map<String, SourceState> previousDatasetStatesByUrns;

  @Getter
  private final List<WorkUnitState> previousWorkUnitStates = Lists.newArrayList();

  @Getter @Setter
  private SharedResourcesBroker<GobblinScopeTypes> broker;

  /**
   * Default constructor.
   */
  public SourceState() {
    this.previousDatasetStatesByUrns = ImmutableMap.of();
  }

  /**
   * Constructor.
   *
   * @param properties job configuration properties
   * @param previousWorkUnitStates an {@link Iterable} of {@link WorkUnitState}s of the previous job run
   */
  public SourceState(State properties, Iterable<WorkUnitState> previousWorkUnitStates) {
    super.addAll(properties);
    this.previousDatasetStatesByUrns = ImmutableMap.of();
    for (WorkUnitState workUnitState : previousWorkUnitStates) {
      this.previousWorkUnitStates.add(new ImmutableWorkUnitState(workUnitState));
    }
  }

  /**
   * Constructor.
   *
   * @param properties job configuration properties
   * @param previousDatasetStatesByUrns {@link SourceState} of the previous job run
   * @param previousWorkUnitStates an {@link Iterable} of {@link WorkUnitState}s of the previous job run
   */
  public SourceState(State properties, Map<String, ? extends SourceState> previousDatasetStatesByUrns,
      Iterable<WorkUnitState> previousWorkUnitStates) {
    super.addAll(properties.getProperties());
    this.previousDatasetStatesByUrns = ImmutableMap.copyOf(previousDatasetStatesByUrns);
    for (WorkUnitState workUnitState : previousWorkUnitStates) {
      this.previousWorkUnitStates.add(new ImmutableWorkUnitState(workUnitState));
    }
  }

  /**
   * Get the {@link SourceState} of the previous job run.
   *
   * <p>
   *   This is a convenient method for existing jobs that do not use the new feature that allows output data to
   *   be committed on a per-dataset basis. Use of this method assumes that the job deals with a single dataset,
   *   which uses the default data URN defined by {@link ConfigurationKeys#DEFAULT_DATASET_URN}.
   * </p>
   *
   * @return {@link SourceState} of the previous job run or {@code null} if no previous {@link SourceState} is found
   */
  public SourceState getPreviousSourceState() {
    return getPreviousDatasetState(ConfigurationKeys.DEFAULT_DATASET_URN);
  }

  /**
   * Get the state (in the form of a {@link SourceState}) of a dataset identified by a dataset URN
   * of the previous job run.
   *
   * @param datasetUrn the dataset URN
   * @return the dataset state (in the form of a {@link SourceState}) of the previous job run
   *         or {@code null} if no previous dataset state is found for the given dataset URN
   */
  public SourceState getPreviousDatasetState(String datasetUrn) {
    if (!this.previousDatasetStatesByUrns.containsKey(datasetUrn)) {
      return null;
    }
    return new ImmutableSourceState(this.previousDatasetStatesByUrns.get(datasetUrn));
  }

  /**
   * Get a {@link Map} from dataset URNs (as being specified by {@link ConfigurationKeys#DATASET_URN_KEY}
   * to the {@link WorkUnitState} with the dataset URNs.
   *
   * <p>
   *   {@link WorkUnitState}s that do not have {@link ConfigurationKeys#DATASET_URN_KEY} set will be added
   *   to the dataset state belonging to {@link ConfigurationKeys#DEFAULT_DATASET_URN}.
   * </p>
   *
   * @return a {@link Map} from dataset URNs to the {@link WorkUnitState} with the dataset URNs
   */
  public Map<String, Iterable<WorkUnitState>> getPreviousWorkUnitStatesByDatasetUrns() {
    Map<String, Iterable<WorkUnitState>> previousWorkUnitStatesByDatasetUrns = Maps.newHashMap();

    for (WorkUnitState workUnitState : this.previousWorkUnitStates) {
      String datasetUrn =
          workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
      if (!previousWorkUnitStatesByDatasetUrns.containsKey(datasetUrn)) {
        previousWorkUnitStatesByDatasetUrns.put(datasetUrn, Lists.<WorkUnitState> newArrayList());
      }
      ((List<WorkUnitState>) previousWorkUnitStatesByDatasetUrns.get(datasetUrn)).add(workUnitState);
    }

    return ImmutableMap.copyOf(previousWorkUnitStatesByDatasetUrns);
  }

  /**
   * Create a new properly populated {@link Extract} instance.
   *
   * <p>
   *   This method should always return a new unique {@link Extract} instance.
   * </p>
   *
   * @param type {@link org.apache.gobblin.source.workunit.Extract.TableType}
   * @param namespace namespace of the table this extract belongs to
   * @param table name of the table this extract belongs to
   * @return a new unique {@link Extract} instance
   *
   * @Deprecated Use {@link org.apache.gobblin.source.extractor.extract.AbstractSource#createExtract(
   * org.apache.gobblin.source.workunit.Extract.TableType, String, String)}
   */
  @Deprecated
  public synchronized Extract createExtract(Extract.TableType type, String namespace, String table) {
    Extract extract = new Extract(this, type, namespace, table);
    while (EXTRACT_SET.contains(extract)) {
      if (Strings.isNullOrEmpty(extract.getExtractId())) {
        extract.setExtractId(DTF.print(new DateTime()));
      } else {
        DateTime extractDateTime = DTF.parseDateTime(extract.getExtractId());
        extract.setExtractId(DTF.print(extractDateTime.plusSeconds(1)));
      }
    }
    EXTRACT_SET.add(extract);
    return extract;
  }

  /**
   * Create a new {@link WorkUnit} instance from a given {@link Extract}.
   *
   * @param extract given {@link Extract}
   * @return a new {@link WorkUnit} instance
   *
   * @deprecated Properties in SourceState should not added to a WorkUnit. Having each WorkUnit contain a copy of
   * SourceState is a waste of memory. Use {@link WorkUnit#create(Extract)}.
   */
  @Deprecated
  public WorkUnit createWorkUnit(Extract extract) {
    return new WorkUnit(this, extract);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.previousWorkUnitStates.size());
    for (WorkUnitState state : this.previousWorkUnitStates) {
      state.write(out);
    }
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      WorkUnitState workUnitState = new WorkUnitState();
      workUnitState.readFields(in);
      this.previousWorkUnitStates.add(new ImmutableWorkUnitState(workUnitState));
    }
    super.readFields(in);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof SourceState)) {
      return false;
    }

    SourceState other = (SourceState) object;
    return super.equals(other) && this.previousDatasetStatesByUrns.equals(other.previousDatasetStatesByUrns)
        && this.previousWorkUnitStates.equals(other.previousWorkUnitStates);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + this.previousDatasetStatesByUrns.hashCode();
    result = prime * result + this.previousWorkUnitStates.hashCode();
    return result;
  }

  /**
   * An immutable version of {@link SourceState} that disables all methods that may change the
   * internal state of a {@link SourceState}.
   */
  private static class ImmutableSourceState extends SourceState {

    public ImmutableSourceState(SourceState sourceState) {
      super(sourceState, sourceState.previousDatasetStatesByUrns, sourceState.previousWorkUnitStates);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setId(String id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setProp(String key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void appendToListProp(String key, String value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(State otherState) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(Properties properties) {
      throw new UnsupportedOperationException();
    }
  }
}
