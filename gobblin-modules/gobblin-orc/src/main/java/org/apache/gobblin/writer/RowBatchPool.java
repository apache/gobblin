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

package org.apache.gobblin.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/***
 * Maintains a pool of row batches per orc schema.
 * Expires row batches which have not been accessed for {@code ROW_BATCH_EXPIRY_INTERVAL}
 */
@Slf4j
public class RowBatchPool {
    static final String PREFIX = "orc.row.batch.";
    static final String ENABLE_ROW_BATCH_POOL = PREFIX + "enable";

    static final String ROW_BATCH_EXPIRY_INTERVAL = PREFIX + "expiry.interval.secs";
    static final int DEFAULT_ROW_BATCH_EXPIRY_INTERVAL = 10;

    static final String ROW_BATCH_EXPIRY_PERIOD = PREFIX + "expiry.period.secs";
    static final int DEFAULT_ROW_BATCH_EXPIRY_PERIOD = 1;

    private static RowBatchPool INSTANCE;

    private final Map<TypeDescription, LinkedList<RowBatchHolder>> rowBatches;
    private final ScheduledExecutorService rowBatchExpiryThread;
    private final long rowBatchExpiryInterval;

    private RowBatchPool(State properties) {
        rowBatches = Maps.newHashMap();
        rowBatchExpiryThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(true).build());
        // expire row batches older N secs
        rowBatchExpiryInterval = properties.getPropAsLong(ROW_BATCH_EXPIRY_INTERVAL, DEFAULT_ROW_BATCH_EXPIRY_INTERVAL);
        // check every N secs
        long rowBatchExpiryPeriod = properties.getPropAsLong(ROW_BATCH_EXPIRY_PERIOD, DEFAULT_ROW_BATCH_EXPIRY_PERIOD);
        rowBatchExpiryThread.scheduleAtFixedRate(
                rowBatchExpiryFn(), rowBatchExpiryPeriod, rowBatchExpiryPeriod, TimeUnit.SECONDS);
    }

    private Runnable rowBatchExpiryFn() {
        return () -> {
            synchronized (rowBatches) {
                for (Map.Entry<TypeDescription, LinkedList<RowBatchHolder>> e : rowBatches.entrySet()) {
                    LinkedList<RowBatchHolder> val = e.getValue();
                    val.removeIf(this::candidateForRemoval);
                }
            }
        };
    }

    private boolean candidateForRemoval(RowBatchHolder batch) {
        long expiryInterval = TimeUnit.SECONDS.toMillis(rowBatchExpiryInterval);
        long interval = System.currentTimeMillis() - batch.lastUsed;
        if (interval > expiryInterval) {
            log.info("Expiring row batch {} as it has not been accessed since {} ms",
                    System.identityHashCode(batch.rowBatch), interval);
            return true;
        } else {
            return false;
        }
    }

    private static class RowBatchHolder {
        long lastUsed;
        VectorizedRowBatch rowBatch;

        private RowBatchHolder(VectorizedRowBatch rowBatch, long currentTimeMillis) {
            this.rowBatch = rowBatch;
            this.lastUsed = currentTimeMillis;
        }
    }

    public synchronized static RowBatchPool instance(State properties) {
        if (INSTANCE == null) {
            INSTANCE = new RowBatchPool(properties);
        }
        return INSTANCE;
    }

    public VectorizedRowBatch getRowBatch(TypeDescription schema, int batchSize) {
        synchronized (rowBatches) {
            LinkedList<RowBatchHolder> vals = rowBatches.get(schema);
            VectorizedRowBatch rowBatch;

            if (vals == null || vals.size() == 0) {
                rowBatch = schema.createRowBatch(batchSize);
                log.info("Creating new row batch {}", System.identityHashCode(rowBatch));
            } else {
                rowBatch = vals.removeLast().rowBatch;
                log.info("Using existing row batch {}", System.identityHashCode(rowBatch));
            }
            return rowBatch;
        }
    }

    public void recycle(TypeDescription schema, VectorizedRowBatch rowBatch) {
        log.info("Recycling row batch {}", System.identityHashCode(rowBatch));
        synchronized (rowBatches) {
            rowBatches.computeIfAbsent(schema, ignore -> Lists.newLinkedList());
            LinkedList<RowBatchHolder> vals = rowBatches.get(schema);
            vals.add(new RowBatchHolder(rowBatch, System.currentTimeMillis()));
        }
    }
}
