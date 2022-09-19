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

package org.apache.gobblin.runtime.embedded;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.api.client.util.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.converter.GobblinMetricsPinotFlattenerConverter;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.SchemaCheckedCopySource;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.util.HiveJdbcConnector;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filesystem.DataFileVersionStrategy;


public class EmbeddedGobblinDistcpTest {
  private HiveJdbcConnector jdbcConnector;
  private IMetaStoreClient metaStoreClient;
  private static final String TEST_DB = "testdb";
  private static final String TEST_TABLE = "test_table";
  private static final String TARGET_PATH = "/tmp/target";
  private static final String TARGET_DB = "target";

  @BeforeClass
  public void setup() throws Exception {
    try {
      HiveConf hiveConf = new HiveConf();
      // Start a Hive session in this thread and register the UDF
      SessionState.start(hiveConf);
      SessionState.get().initTxnMgr(hiveConf);
      metaStoreClient = new HiveMetaStoreClient(new HiveConf());
      jdbcConnector = HiveJdbcConnector.newEmbeddedConnector(2);
    } catch (HiveException he) {
      throw new RuntimeException("Failed to start Hive session.", he);
    } catch (SQLException se) {
      throw new RuntimeException("Cannot initialize the jdbc-connector due to: ", se);
    }
  }

  @Test
  public void test() throws Exception {
    String fileName = "file";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    tmpFile.createNewFile();

    FileOutputStream os = new FileOutputStream(tmpFile);
    for (int i = 0; i < 100; i++) {
      os.write("myString".getBytes(Charsets.UTF_8));
    }
    os.close();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.run();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());
  }

  @Test
  public void hiveTest() throws Exception {
    Statement statement = jdbcConnector.getConnection().createStatement();

    // Start from a fresh Hive backup: No DB, no table.
    // Create a DB.
    statement.execute("CREATE database if not exists " + TEST_DB);

    // Create a table.
    String tableCreationSQL = "CREATE TABLE IF NOT EXISTS $testdb.$test_table (id int, name String)\n" + "ROW FORMAT DELIMITED\n"
        + "FIELDS TERMINATED BY '\\t'\n" + "LINES TERMINATED BY '\\n'\n" + "STORED AS TEXTFILE";
    statement.execute(tableCreationSQL.replace("$testdb",TEST_DB).replace("$test_table", TEST_TABLE));

    // Insert data
    String dataInsertionSQL = "INSERT INTO TABLE $testdb.$test_table VALUES (1, 'one'), (2, 'two'), (3, 'three')";
    statement.execute(dataInsertionSQL.replace("$testdb",TEST_DB).replace("$test_table", TEST_TABLE));
    String templateLoc = "templates/hiveDistcp.template";

    // Either of the "from" or "to" will be used here since it is a Hive Distcp.
    EmbeddedGobblinDistcp embeddedHiveDistcp =
        new EmbeddedGobblinDistcp(templateLoc, new Path("a"), new Path("b"));
    embeddedHiveDistcp.setConfiguration("hive.dataset.copy.target.database", TARGET_DB);
    embeddedHiveDistcp.setConfiguration("hive.dataset.copy.target.table.prefixReplacement", TARGET_PATH);

    String dbPathTemplate = "/$testdb.db/$test_table";
    String rootPathOfSourceDate = metaStoreClient.getConfigValue("hive.metastore.warehouse.dir", "")
        .concat(dbPathTemplate.replace("$testdb", TEST_DB).replace("$test_table",TEST_TABLE)
    );
    embeddedHiveDistcp.setConfiguration("hive.dataset.copy.target.table.prefixToBeReplaced", rootPathOfSourceDate);
    embeddedHiveDistcp.run();

    // Verify the table is existed in the target and file exists in the target location.
    metaStoreClient.tableExists(TARGET_DB, TEST_TABLE);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.exists(new Path(TARGET_PATH));
  }

  // Tearing down the Hive components from derby driver if there's anything generated through the test.
  @AfterClass
  public void hiveTearDown() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path targetPath = new Path(TARGET_PATH);
    if (fs.exists(targetPath)) {
      fs.delete(targetPath, true);
    }

    if (metaStoreClient != null) {
      // Clean out all tables in case there are any, to avoid db-drop failure.
      for (String tblName : metaStoreClient.getAllTables(TEST_DB)) {
        metaStoreClient.dropTable(TEST_DB, tblName);
      }

      if (metaStoreClient.getAllDatabases().contains(TEST_DB)) {
        metaStoreClient.dropDatabase(TEST_DB);
      }

      // Clean the target table and DB
      if (metaStoreClient.tableExists("target", TEST_TABLE)) {
        metaStoreClient.dropTable("target", TEST_TABLE, true, true);
      }
      if (metaStoreClient.getAllDatabases().contains(TARGET_DB)) {
        metaStoreClient.dropDatabase(TARGET_DB);
      }
      metaStoreClient.close();
    }

    jdbcConnector.close();
  }

  @Test
  public void testCheckSchema() throws Exception {
    Schema schema = null;
    try (InputStream is = GobblinMetricsPinotFlattenerConverter.class.getClassLoader().getResourceAsStream("avroSchemaManagerTest/expectedSchema.avsc")) {
      schema = new Schema.Parser().parse(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
    String fileName = "file.avro";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    tmpFile.createNewFile();

    GenericDatumWriter<GenericRecord> datumWriter = new
        GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, tmpFile);
    for(int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("foo", i);
      dataFileWriter.append(record);
    }

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setConfiguration(CopySource.SCHEMA_CHECK_ENABLED, "true");
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, SchemaCheckedCopySource.class.getName());
    embedded.setConfiguration(ConfigurationKeys.AVRO_SCHEMA_CHECK_STRATEGY, "org.apache.gobblin.util.schema_check.AvroSchemaCheckDefaultStrategy");
    //test when schema is not the expected one, the job will be aborted.
    embedded.setConfiguration(ConfigurationKeys.COPY_EXPECTED_SCHEMA, "{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo1\",\"type\":[\"null\",\"int\"],\"doc\":\"this is for test\",\"default\":null}]}");
    JobExecutionResult result = embedded.run();
    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());
    embedded.setConfiguration(ConfigurationKeys.COPY_EXPECTED_SCHEMA, "{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"string\",\"int\"],\"doc\":\"this is for test\",\"default\":null}]}");
    result = embedded.run();
    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    //test when schema is the expected one, the job will succeed.
    embedded.setConfiguration(ConfigurationKeys.COPY_EXPECTED_SCHEMA, "{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":[{\"name\":\"foo\",\"type\":[\"null\",\"int\"],\"doc\":\"this is for test\",\"default\":null}]}");
    result = embedded.run();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());


  }

  @Test
  public void testWithVersionPreserve() throws Exception {
    String fileName = "file";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    tmpFile.createNewFile();

    FileOutputStream os = new FileOutputStream(tmpFile);
    for (int i = 0; i < 100; i++) {
      os.write("myString".getBytes(Charsets.UTF_8));
    }
    os.close();

    MyDataFileVersion versionStrategy = new MyDataFileVersion();
    versionStrategy.setVersion(new Path(tmpFile.getAbsolutePath()), 123L);

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.setConfiguration(DataFileVersionStrategy.DATA_FILE_VERSION_STRATEGY_KEY, MyDataFileVersion.class.getName());
    embedded.setConfiguration(CopyConfiguration.PRESERVE_ATTRIBUTES_KEY, "v");
    embedded.run();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());
    Assert.assertEquals((long) versionStrategy.getVersion(new Path(tmpTarget.getAbsolutePath(), fileName)), 123l);
  }

  @Test
  public void testWithModTimePreserve() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileName = "file";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    Assert.assertTrue(tmpFile.createNewFile());

    FileOutputStream os = new FileOutputStream(tmpFile);
    for (int i = 0; i < 100; i++) {
      os.write("myString".getBytes(Charsets.UTF_8));
    }
    os.close();

    long originalModTime = fs.getFileStatus(new Path(tmpFile.getPath())).getModificationTime();
    Assert.assertNotNull(originalModTime);

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertFalse(new File(tmpTarget, fileName).exists());

    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.setConfiguration(CopyConfiguration.PRESERVE_ATTRIBUTES_KEY, "t");
    embedded.run();

    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());
    Assert.assertEquals(fs.getFileStatus(new Path(new File(tmpTarget, fileName).getAbsolutePath())).getModificationTime()
        , originalModTime);
  }

  @Test
  public void testWithModTimePreserveNegative() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileName = "file_oh";

    File tmpSource = Files.createTempDir();
    tmpSource.deleteOnExit();
    File tmpTarget = Files.createTempDir();
    tmpTarget.deleteOnExit();

    File tmpFile = new File(tmpSource, fileName);
    Assert.assertTrue(tmpFile.createNewFile());

    FileOutputStream os = new FileOutputStream(tmpFile);
    for (int i = 0; i < 100; i++) {
      os.write("myString".getBytes(Charsets.UTF_8));
    }
    os.close();

    long originalModTime = fs.getFileStatus(new Path(tmpFile.getPath())).getModificationTime();
    Assert.assertFalse(new File(tmpTarget, fileName).exists());
    // Give a minimal gap between file creation and copy
    Thread.sleep(1000);

    // Negative case, not preserving the timestamp.
    tmpTarget.deleteOnExit();
    EmbeddedGobblinDistcp embedded = new EmbeddedGobblinDistcp(new Path(tmpSource.getAbsolutePath()),
        new Path(tmpTarget.getAbsolutePath()));
    embedded.setLaunchTimeout(30, TimeUnit.SECONDS);
    embedded.run();
    Assert.assertTrue(new File(tmpSource, fileName).exists());
    Assert.assertTrue(new File(tmpTarget, fileName).exists());
    long newModTime = fs.getFileStatus(new Path(new File(tmpTarget, fileName).getAbsolutePath())).getModificationTime();
    Assert.assertTrue(newModTime != originalModTime);
  }

  public static class MyDataFileVersion implements DataFileVersionStrategy<Long>, DataFileVersionStrategy.DataFileVersionFactory<Long> {
    private static final Map<Path, Long> versions = new HashMap<>();

    @Override
    public DataFileVersionStrategy<Long> createDataFileVersionStrategy(FileSystem fs, Config config) {
      return this;
    }

    @Override
    public Long getVersion(Path path)
        throws IOException {
      return versions.get(PathUtils.getPathWithoutSchemeAndAuthority(path));
    }

    @Override
    public boolean setVersion(Path path, Long version)
        throws IOException {
      versions.put(PathUtils.getPathWithoutSchemeAndAuthority(path), version);
      return true;
    }

    @Override
    public boolean setDefaultVersion(Path path)
        throws IOException {
      return false;
    }

    @Override
    public Set<Characteristic> applicableCharacteristics() {
      return Sets.newHashSet(Characteristic.SETTABLE);
    }
  }

}