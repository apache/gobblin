package gobblin.test.common;

import java.io.IOException;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for configuring test Log4j logging */
public class Log4jUtils {
  public static Logger LOG = LoggerFactory.getLogger(Log4jUtils.class);

  public static final Layout STANDARD_LAYOUT =
      new PatternLayout("%d{ISO8601} +%r [%p] {%c{2}} (%t)  %m%n");

  /** Helper method to change the Log4j logging level given a SLF4J facade */
  public static void changeLoggingLevel(Logger slf4jLogger, Level log4jLevel) {
    org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(slf4jLogger.getName());
    log4jLogger.setLevel(log4jLevel);
  }

  /** Helper method to change the Log4j logging level given a SLF4J facade */
  public static void changeLoggingLevel(Logger slf4jLogger, String log4jLevel) {
    changeLoggingLevel(slf4jLogger, Level.toLevel(log4jLevel));
  }

  /**
   * Programatically configures the root Log4j logger to send the logs to a file. This is useful
   * for tests where console logging may not be visible or may be too verbose. Note that this method
   * will leave the file. Typically, the file should be under /tmp/ so it gets removed eventually.
   *
   * @param  filePath     the logging file to write to; typically, under /tmp/
   * @param  log4jLevel   the string constant for logging level, e.g. DEBUG, INFO, ERROR
   */
  public static void logToFile(String filePath, String log4jLevel) {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    try {
      Appender fileAppender = new FileAppender(STANDARD_LAYOUT, filePath);
      rootLogger.addAppender(fileAppender);
      rootLogger.setLevel(Level.toLevel(log4jLevel));
    }
    catch (IOException e) {
      LOG.error("Unable to setup logging: "+ e, e);
    }
  }

  /**
   * Programatically configures the root Log4j logger to send the logs to console.
   *
   * @param  log4jLevel   the string constant for logging level, e.g. DEBUG, INFO, ERROR
   */
  public static void logToConsole(String log4jLevel) {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    Appender consoleAppender = new ConsoleAppender(STANDARD_LAYOUT);
    rootLogger.addAppender(consoleAppender);
    rootLogger.setLevel(Level.toLevel(log4jLevel));
  }

  /**
   * Convenience method to setup logging for a test class. It will create a timestamped log file
   * under /tmp/ with the test class name.
   *
   * <p>NOTE: this method will reset any existing Log4j configs!
   *
   * @param testClass       the test class for which logging is configured; it will be used for the
   *                        log file name.
   * @param log4jLevel      the string constant for logging level, e.g. DEBUG, INFO, ERROR
   * @param logToConsole    if true, enable logging to console as well
   */
  public static void configureLoggingForTest(Class<?> testClass, String log4jLevel,
      boolean logToConsole) {
    DateTimeFormatter fileTimestampFormat = DateTimeFormat.forPattern("yyyyMMdd-HHmmss.SSS");
    String logFilePath = String.format("/tmp/%s.%s.log", testClass.getSimpleName(),
        fileTimestampFormat.print(new DateTime()));
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    rootLogger.removeAllAppenders();
    logToFile(logFilePath, log4jLevel);
    if (logToConsole) {
      logToConsole(log4jLevel);
    }

  }

}
