package gobblin.capability;

import gobblin.configuration.State;
import java.util.Collection;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class EncryptionCapabilityParserTest {
  private EncryptionCapabilityParser parser;

  @BeforeTest
  public void initParser() {
    parser = new EncryptionCapabilityParser();
  }

  @Test
  public void testValidConfigOneBranch() {
    testWithWriterPrefix(1, 0);
  }

  @Test
  public void testValidConfigSeparateBranch() {
    testWithWriterPrefix(3, 1);
  }

  private void testWithWriterPrefix(int numBranches, int branch) {
    String branchString = "";
    if (numBranches > 1) {
      branchString = String.format(".%d", branch);
    }

    Properties properties = new Properties();
    properties.put("writer.encrypt"  + branchString, "any");
    properties.put("writer.encrypt.ks_path"  + branchString, "/tmp/foobar");
    properties.put("writer.encrypt.ks_password"  + branchString, "abracadabra");

    State s = new State(properties);

    Collection<CapabilityParser.CapabilityRecord> recordColl = parser.parseForBranch(s, numBranches, branch);
    Assert.assertEquals(recordColl.size(), 1, "Expected parser to only return one record");

    CapabilityParser.CapabilityRecord record = recordColl.iterator().next();
    Assert.assertTrue(record.isConfigured(), "Expected encryption to be configured");
    Assert.assertEquals(record.getParameters().get("type"), "any");
    Assert.assertEquals(record.getParameters().get("ks_path"), "/tmp/foobar");
    Assert.assertEquals(record.getParameters().get("ks_password"), "abracadabra");



  }
}
