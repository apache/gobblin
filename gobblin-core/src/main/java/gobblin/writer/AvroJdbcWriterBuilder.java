package gobblin.writer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroJdbcWriterBuilder extends PartitionAwareDataWriterBuilder<Schema, GenericRecord> {

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return false;
  }

  @Override
  public DataWriter<GenericRecord> build() throws IOException {
    return new AvroJdbcWriter(this);
//    String tableName = "stage_user";
//    JdbcProvider jdbcProvider = createJdbcSource();
//    DataWriter<GenericRecord> writer = new AvroJdbcWriter(jdbcProvider, tableName);
//    return writer;
  }

//  private JdbcProvider createJdbcSource() {
////    String driver = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_DRIVER);
////    String userName = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
////    String password =
////        PasswordManager.getInstance(this.workUnit).readPassword(
////            this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
////    String connectionUrl = this.getConnectionUrl();
//
//    String driver = "com.mysql.jdbc.Driver";
//    String userName = "scott";
//    String password = "tiger";
//    String connectionUrl = "jdbc:mysql://localhost:3306/gobblin_test";
//
//    return new JdbcProvider(driver, connectionUrl, userName, password, 1, 10000);
//  }
}