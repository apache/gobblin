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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RowBatchPoolTest {
    @Test
    public void testExpiry() throws Exception {
        State state = WorkUnit.createEmpty();
        RowBatchPool instance = RowBatchPool.instance(state);
        TypeDescription schema = TypeDescription.fromString("struct<a:int,b:string>");
        VectorizedRowBatch rowBatch1 = instance.getRowBatch(schema, 1024);
        instance.recycle(schema, rowBatch1);
        VectorizedRowBatch rowBatch2 = instance.getRowBatch(schema, 1024);
        // existing rowbatch is fetched from pool
        Assert.assertEquals(rowBatch1, rowBatch2);

        // since the pool has no existing rowbatch, a new one is created
        VectorizedRowBatch rowBatch3 = instance.getRowBatch(schema, 1024);
        Assert.assertNotEquals(rowBatch1, rowBatch3);

        // recyle fetched rowbatches
        instance.recycle(schema, rowBatch2);
        instance.recycle(schema, rowBatch3);

        // wait for their expiry
        Thread.sleep(RowBatchPool.DEFAULT_ROW_BATCH_EXPIRY_INTERVAL * 1000L);
        VectorizedRowBatch rowBatch4 = instance.getRowBatch(schema, 1024);
        // new rowbatch is created, all old ones are expired
        Assert.assertNotEquals(rowBatch1, rowBatch4);
    }
}
