package org.apache.gobblin.metastore;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link DatabaseJobHistoryStore} V1.0.1.
 *
 */
@Test(groups = {"gobblin.metastore"})
public class DatabaseJobHistoryStoreV103Test extends DatabaseJobHistoryStoreTest {
    @Override
    protected String getVersion() {
        return "1.0.3";
    }
}
