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

package gobblin.configuration;

import gobblin.source.workunit.Extract;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.source.workunit.WorkUnit;


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

  private final List<WorkUnitState> previousTaskStates = Lists.newArrayList();
  private static final Set<Extract> extractSet = Sets.newConcurrentHashSet();
  private static final DateTimeFormatter DTF =
      DateTimeFormat.forPattern("yyyyMMddHHmmss").withLocale(Locale.US).withZone(DateTimeZone.UTC);

  /**
   * Default constructor.
   */
  public SourceState() {
  }

  /**
   * Constructor.
   *
   * @param properties job configuration properties
   * @param previousTaskStates list of previous task states
   */
  public SourceState(State properties, List<WorkUnitState> previousTaskStates) {
    addAll(properties);
    this.previousTaskStates.addAll(previousTaskStates);
  }

  /**
   * Get a (possibly empty) list of {@link WorkUnitState}s from the previous job run.
   *
   * @return (possibly empty) list of {@link WorkUnitState}s from the previous job run
   */
  public List<WorkUnitState> getPreviousWorkUnitStates() {
    return ImmutableList.<WorkUnitState>builder().addAll(this.previousTaskStates).build();
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
      DateTime extractDateTime = DTF.parseDateTime(extract.getExtractId());
      extract.setExtractId(DTF.print(extractDateTime.plusSeconds(1)));
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
  public void write(DataOutput out)
      throws IOException {
    out.writeInt(this.previousTaskStates.size());
    for (WorkUnitState state : this.previousTaskStates) {
      state.write(out);
    }
    super.write(out);
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      WorkUnitState state = new WorkUnitState();
      state.readFields(in);
      this.previousTaskStates.add(state);
    }
    super.readFields(in);
  }
}
