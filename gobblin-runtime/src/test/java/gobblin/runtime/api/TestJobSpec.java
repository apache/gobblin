package gobblin.runtime.api;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.configuration.ConfigurationKeys;

/**
 * Unit tests for {@link JobSpec}
 */
public class TestJobSpec {

  @Test
  public void testBuilder() throws URISyntaxException {
    JobSpec.Builder b = new JobSpec.Builder("test:job");

    JobSpec js1 = b.build();

    Assert.assertEquals(js1.getUri().toString(), "test:job");
    Assert.assertEquals(js1.getVersion(), "1");
    Assert.assertNotNull(js1.getDescription());
    Assert.assertTrue(js1.getDescription().contains("test:job"));
    Assert.assertEquals(js1.getConfig().entrySet().size(), 0);
    Assert.assertEquals(js1.getConfigAsProperties().size(), 0);


    Properties props = new Properties();
    props.put("a1", "a_value");
    props.put("a2.b", "1");
    props.put("a2.c.d", "12.34");
    props.put("a2.c.d2", "true");

    b = new JobSpec.Builder("test:job2")
        .withVersion("2")
        .withDescription("A test job")
        .withConfigAsProperties(props);

    JobSpec js2 = b.build();

    Assert.assertEquals(js2.getUri().toString(), "test:job2");
    Assert.assertEquals(js2.getVersion(), "2");
    Assert.assertEquals(js2.getDescription(), "A test job");
    Assert.assertEquals(js2.getConfig().getString("a1"), "a_value");
    Assert.assertEquals(js2.getConfig().getLong("a2.b"), 1L);
    Assert.assertEquals(js2.getConfig().getDouble("a2.c.d"), 12.34);
    Assert.assertTrue(js2.getConfig().getBoolean("a2.c.d2"));

    Config cfg =
        ConfigFactory.empty()
                     .withValue("a1", ConfigValueFactory.fromAnyRef("some_string"))
                     .withValue("a2.b", ConfigValueFactory.fromAnyRef(-1))
                     .withValue("a2.c.d", ConfigValueFactory.fromAnyRef(1.2))
                     .withValue("a2.e.f", ConfigValueFactory.fromAnyRef(true));

    b = new JobSpec.Builder("test:job")
        .withVersion("3")
        .withDescription("A test job")
        .withConfig(cfg);

    JobSpec js3 = b.build();

    Assert.assertEquals(js3.getUri().toString(), "test:job");
    Assert.assertEquals(js3.getVersion(), "3");
    Assert.assertEquals(js3.getDescription(), "A test job");
    Assert.assertEquals(js3.getConfigAsProperties().getProperty("a1"), "some_string");
    Assert.assertEquals(js3.getConfigAsProperties().getProperty("a2.b"), "-1");
    Assert.assertEquals(js3.getConfigAsProperties().getProperty("a2.c.d"), "1.2");
    Assert.assertEquals(js3.getConfigAsProperties().getProperty("a2.e.f"), "true");

    Config cfg2 =
        ConfigFactory.empty()
                     .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob"))
                     .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef("myGroup"))
                     .withValue(ConfigurationKeys.JOB_DESCRIPTION_KEY, ConfigValueFactory.fromAnyRef("Awesome job"));

    b = new JobSpec.Builder().withConfig(cfg2);
    JobSpec js4 = b.build();

    Assert.assertEquals(js4.getUri(), new URI(JobSpec.Builder.DEFAULT_JOB_CATALOG_SCHEME +
                        ":/myGroup/myJob"));
    Assert.assertEquals(js4.getDescription(), "Awesome job");

    b = new JobSpec.Builder().withConfig(cfg2).withJobCatalogURI("my-jobs:/");
    JobSpec js5 = b.build();
    Assert.assertEquals(js5.getUri(), new URI("my-jobs:/myGroup/myJob"));
  }
}
