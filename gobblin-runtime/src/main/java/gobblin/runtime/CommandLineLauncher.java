package gobblin.runtime;

import gobblin.runtime.local.LocalJobLauncher;
import gobblin.util.SchedulerUtils;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Launcher for creating a Gobblin job from command line or IDE.
 */
public class CommandLineLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(CommandLineLauncher.class);

  public static void main(String[] args) {

    if(args.length < 1) {
      throw new RuntimeException("Missing argument. Expected path to configuration file.");
    }

    try {
      // Load framework configuration properties
      PropertiesConfiguration configT = new PropertiesConfiguration();
      configT.load(args[0]);
      Configuration config = configT.interpolatedConfiguration();

      Properties properties = ConfigurationConverter.getProperties(config);

      LOG.info(properties.toString());

      for (Properties jobProps : SchedulerUtils.loadJobConfigs(properties)) {
        LOG.info("Running job.");
        new LocalJobLauncher(jobProps).launchJob(null);
      }
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    } finally {
    }

  }

}
