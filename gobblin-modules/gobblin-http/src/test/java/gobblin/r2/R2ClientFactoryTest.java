package gobblin.r2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.curator.test.TestingServer;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.SettableFuture;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import junit.framework.Assert;

import gobblin.security.ssl.SSLContextFactory;


@Test
public class R2ClientFactoryTest {

  public void testHttpClient() {
    R2ClientFactory factory = new R2ClientFactory(R2ClientFactory.Schema.HTTP);

    Map<String, Object> values = new HashMap<>();
    // No SSL
    Client client = factory.createInstance(ConfigFactory.parseMap(values));
    shutdown(client);

    // With SSL
    values.put(R2ClientFactory.SSL_ENABLED, true);
    values.put(SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(SSLContextFactory.KEY_STORE_TYPE, "PKCS12");

    try {
      factory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail();
    } catch (Exception e) {
      // OK
    }
  }

  public void testD2Client()
      throws Exception {
    R2ClientFactory factory = new R2ClientFactory(R2ClientFactory.Schema.D2);
    TestingServer zkServer = new TestingServer(-1);

    Map<String, Object> values = new HashMap<>();
    values.put("d2.zkHosts", zkServer.getConnectString());
    // No SSL
    Client client = factory.createInstance(ConfigFactory.parseMap(values));
    shutdown(client);

    // With SSL
    final String confPrefix = "d2.";
    values.put(confPrefix + R2ClientFactory.SSL_ENABLED, true);
    values.put(confPrefix + SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(confPrefix + SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(confPrefix + SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(confPrefix + SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(confPrefix + SSLContextFactory.KEY_STORE_TYPE, "PKCS12");

    try {
      factory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail("Unexpected config exception");
    } catch (Exception e) {
      // OK
    }

    zkServer.close();
  }

  private void shutdown(Client client) {
    final SettableFuture<None> future = SettableFuture.create();
    client.shutdown(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        future.setException(e);
      }

      @Override
      public void onSuccess(None result) {
        // OK
        future.set(result);
      }
    });
    try {
      // Synchronously wait for shutdown to complete
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail("Client shutdown failed");
    }
  }
}
