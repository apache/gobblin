package gobblin.admin;

import gobblin.configuration.ConfigurationKeys;
import org.apache.commons.httpclient.HttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.testng.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.testng.AssertJUnit.*;

public class AdminWebServerTest {
    private AdminWebServer server;
    private final String portNumber = "54320";

    @BeforeTest
    public void startServer() {
        Properties properties = new Properties();
        properties.put(ConfigurationKeys.ADMIN_SERVER_PORT_KEY, portNumber);
        server = new AdminWebServer(properties, URI.create("http://foobar:3333"));

        try {
            server.startUp();
        } catch (Exception e) {
            fail(String.format("Exception starting server: %s", e.toString()));
        }
    }

    @AfterTest
    public void stopServer() {
        try {
            server.shutDown();
        } catch (Exception e) {
            // do nothing
        }
    }

    @Test
    public void testGetSettingsJs() throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet getReq = new HttpGet(
                String.format("http://localhost:%s/js/settings.js",
                        portNumber));

        try (CloseableHttpResponse response = client.execute(getReq)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            HttpEntity body = response.getEntity();
            String bodyString = EntityUtils.toString(body);
            assertStringContains("http://foobar", bodyString);
            assertStringContains("3333", bodyString);
        }
    }

    @Test
    public void testGetIndex() throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet getReq = new HttpGet(
                String.format("http://localhost:%s/",
                        portNumber));

        try (CloseableHttpResponse response = client.execute(getReq)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            HttpEntity body = response.getEntity();
            String bodyString = EntityUtils.toString(body);
            assertStringContains("JOB SUMMARY", bodyString);
        }
    }

    private void assertStringContains(String expected, String container) {
        assertTrue(String.format("Expected %s to contain %s", container, expected),
                container.contains(expected));
    }
}
