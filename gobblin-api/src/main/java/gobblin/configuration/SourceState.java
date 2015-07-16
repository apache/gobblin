/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract;


/**
 * A container for all meta data related to a particular source. This includes all properties
 * defined in job configuration files and all properties from tasks of the previous run.
 *
 * <p>
 *   Properties can be overwritten at runtime and persisted upon job completion. Persisted
 *   properties will be loaded in the next run and made available to use by the
 *   {@link gobblin.source.Source}.
 * </p>
 *
 * @author kgoodhop
 */
public class SourceState extends State {

  private static final Set<Extract> extractSet = Sets.newConcurrentHashSet();
  private static final DateTimeFormatter DTF =
      DateTimeFormat.forPattern("yyyyMMddHHmmss").withLocale(Locale.US).withZone(DateTimeZone.UTC);

  private final Optional<SourceState> previousSourceState;
  private final List<WorkUnitState> previousWorkUnitStates = Lists.newArrayList();

  /**
   * Default constructor.
   */
  public SourceState() {
    this.previousSourceState = Optional.absent();
  }

  /**
   * Constructor.
   *
   * @param properties job configuration properties
   * @param previousWorkUnitStates list of {@link WorkUnitState}s of the previous job run
   */
  public SourceState(State properties, List<WorkUnitState> previousWorkUnitStates) {
    addAll(properties);
    this.previousSourceState = Optional.absent();
    for (WorkUnitState workUnitState : previousWorkUnitStates) {
      this.previousWorkUnitStates.add(new ImmutableWorkUnitState(workUnitState));
    }
  }

  /**
   * Constructor.
   *
   * @param properties job configuration properties
   * @param previousSourceState {@link SourceState} of the previous job run
   * @param previousWorkUnitStates list of {@link WorkUnitState}s of the previous job run
   */
  public SourceState(State properties, SourceState previousSourceState, List<WorkUnitState> previousWorkUnitStates) {
    addAll(properties);
    this.previousSourceState = Optional.of(previousSourceState);
    for (WorkUnitState workUnitState : previousWorkUnitStates) {
      this.previousWorkUnitStates.add(new ImmutableWorkUnitState(workUnitState));
    }
  }

  /**
   * Get the {@link SourceState} of the previous job run.
   *
   * @return {@link SourceState} of the previous job run
   */
  public SourceState getPreviousSourceState() {
    return new ImmutableSourceState(this.previousSourceState.or(new SourceState()));
  }

  /**
   * Get a (possibly empty) list of {@link WorkUnitState}s from the previous job run.
   *
   * @return (possibly empty) list of {@link WorkUnitState}s from the previous job run
   */
  public List<WorkUnitState> getPreviousWorkUnitStates() {
    return ImmutableList.<WorkUnitState>builder().addAll(this.previousWorkUnitStates).build();
  }

  /**
   * Create a new properly populated {@link Extract} instance.
   *
   * <p>
   *   This method should always return a new unique {@link Extract} instance.
   * </p>
   *
   * @param type {@link gobblin.source.workunit.Extract.TableType}
   * @param namespace namespace of the table this extract belongs to
   * @param table name of the table this extract belongs to
   * @return a new unique {@link Extract} instance
   */
  public synchronized Extract createExtract(Extract.TableType type, String namespace, String table) {
    Extract extract = new Extract(this, type, namespace, table);
    while (extractSet.contains(extract)) {
      if (Strings.isNullOrEmpty(extract.getExtractId())) {
        extract.setExtractId(DTF.print(new DateTime()));
      } else {
        DateTime extractDateTime = DTF.parseDateTime(extract.getExtractId());
        extract.setExtractId(DTF.print(extractDateTime.plusSeconds(1)));
      }
    }
    extractSet.add(extract);
    return extract;
  }

  /**
   * Create a new {@link WorkUnit} instance from a given {@link Extract}.
   *
   * @param extract given {@link Extract}
   * @return a new {@link WorkUnit} instance
   */
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
    return super.equals(other) && this.previousSourceState.equals(other.previousSourceState) &&
        this.previousWorkUnitStates.equals(other.previousWorkUnitStates);
  }

  /**
   * An immutable version of {@link SourceState} that disables all methods that may change the
   * internal state of a {@link SourceState}.
   */
  private static class ImmutableSourceState extends SourceState {

    public ImmutableSourceState(SourceState sourceState) {
      super(sourceState, sourceState.previousSourceState.or(new SourceState()), sourceState.previousWorkUnitStates);
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