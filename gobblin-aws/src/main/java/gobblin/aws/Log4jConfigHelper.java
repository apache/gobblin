package gobblin.aws;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.io.Closer;


/**
 * A helper class for programmatically configuring log4j.
 *
 * @author Abhishek Tiwari
 */
public class Log4jConfigHelper {
  /**
   * Update the log4j configuration.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jFileName the custom log4j configuration properties file name
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jFileName)
      throws IOException {
    final Closer closer = Closer.create();
    try {
      final InputStream inputStream = closer.register(targetClass.getResourceAsStream("/" + log4jFileName));
      final Properties originalProperties = new Properties();
      originalProperties.load(inputStream);

      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
