package gobblin.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HiveMetaStoreClientFactoryTest {
  @Test
  public void testCreate() throws TException {
    HiveConf hiveConf = new HiveConf();
    HiveMetaStoreClientFactory factory = new HiveMetaStoreClientFactory(hiveConf);
    IMetaStoreClient msc = factory.create();

    String dbName = "test_db";
    String description = "test database";
    String location = "file:/tmp/" + dbName;
    Database db = new Database(dbName, description, location, null);

    msc.dropDatabase(dbName, true, true);
    msc.createDatabase(db);
    db = msc.getDatabase(dbName);
    Assert.assertEquals(db.getName(), dbName);
    Assert.assertEquals(db.getDescription(), description);
    Assert.assertEquals(db.getLocationUri(), location);
  }
}
