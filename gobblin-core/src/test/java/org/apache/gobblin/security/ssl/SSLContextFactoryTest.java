package gobblin.security.ssl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;


@Test
public class SSLContextFactoryTest {
  public void testCreateSSLContext()
      throws IOException {
    Map<String, String> values = new HashMap<>();

    boolean hasException = false;

    // KEY_STORE_FILE_PATH is required
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    hasException = false;
    // TRUST_STORE_FILE_PATH is required
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    values.put(SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(SSLContextFactory.KEY_STORE_TYPE, "XX");

    hasException = false;
    // KEY_STORE_TYPE not legal
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (IllegalArgumentException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    values.put(SSLContextFactory.KEY_STORE_TYPE, "PKCS12");
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail();
    } catch (Exception e) {
      // OK
    }
  }
}
