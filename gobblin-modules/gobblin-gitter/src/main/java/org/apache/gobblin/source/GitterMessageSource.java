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

package org.apache.gobblin.source;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.BigIntegerWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.util.ClassAliasResolver;

import static org.apache.gobblin.source.GitterMessageExtractor.GITTER_EXTRACTOR_DEFAULT;
import static org.apache.gobblin.source.GitterMessageExtractor.GITTER_EXTRACTOR_KEY;


/**
 * An implementation of GitterMessageSource to get work units.
 */
@Slf4j
public class GitterMessageSource extends AbstractSource<Schema, GenericRecord> {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Map<String, Iterable<WorkUnitState>> previousWorkUnits = state.getPreviousWorkUnitStatesByDatasetUrns();
    List<String> gitterRooms = new LinkedList<>(Splitter.on(",").omitEmptyStrings().
        splitToList(state.getProp(GitterMessageExtractor.GITTER_ROOM_NAMES_KEY)));

    Map<String, BigIntegerWatermark> prevHighWatermarks = Maps.newHashMap();
    for (Map.Entry<String, Iterable<WorkUnitState>> entry : previousWorkUnits.entrySet()) {
      Iterable<BigIntegerWatermark> watermarks =
          Iterables.transform(entry.getValue(), new Function<WorkUnitState, BigIntegerWatermark>() {
            @Override
            public BigIntegerWatermark apply(WorkUnitState wus) {
              return wus.getActualHighWatermark(BigIntegerWatermark.class);
            }
          });
      watermarks = Iterables.filter(watermarks, Predicates.notNull());
      List<BigIntegerWatermark> watermarkList = Lists.newArrayList(watermarks);
      if (watermarkList.size() > 0) {
        prevHighWatermarks.put(entry.getKey(), Collections.max(watermarkList));
      }
    }

    Extract extract = createExtract(Extract.TableType.SNAPSHOT_ONLY,
        state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), "GitterOutput");
    List<WorkUnit> workUnits = Lists.newArrayList();

    for (String gitterRoom : gitterRooms) {
      BigIntegerWatermark prevWatermark = prevHighWatermarks.containsKey(gitterRoom) ? prevHighWatermarks.get(gitterRoom) :
          new BigIntegerWatermark(BigInteger.ZERO);
      prevHighWatermarks.remove(gitterRoom);
      log.info(String.format("Creating workunit for room: %s with low watermark: %s", gitterRoom,
          prevWatermark.getValue().toString(16)));
      WorkUnit workUnit = WorkUnit.create(extract, new WatermarkInterval(prevWatermark,
          new BigIntegerWatermark(BigInteger.ZERO)));
      workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, gitterRoom);
      workUnits.add(workUnit);
    }

    for (Map.Entry<String, BigIntegerWatermark> nonProcessedDataset : prevHighWatermarks.entrySet()) {
      WorkUnit workUnit = WorkUnit.create(extract, new WatermarkInterval(nonProcessedDataset.getValue(),
          nonProcessedDataset.getValue()));
      workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, nonProcessedDataset.getKey());
      workUnits.add(workUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    ClassAliasResolver<Extractor> aliasResolver = new ClassAliasResolver<>(Extractor.class);
    Extractor<Schema, GenericRecord> extractor = null;
    try {
      String extractorClass = state.getProp(GITTER_EXTRACTOR_KEY, GITTER_EXTRACTOR_DEFAULT);
      this.log.info("Using extractor class name/alias " + extractorClass);
      extractor = (Extractor<Schema, GenericRecord>) ConstructorUtils.
          invokeConstructor(Class.forName(aliasResolver.resolve(extractorClass)), state);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return extractor;
  }

  @Override
  public void shutdown(SourceState state) {

  }
}