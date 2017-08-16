package gobblin.test.common;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Simple unit tests for {@link Log4jUtils} */
public class TestLog4jUtils {
  org.apache.log4j.Logger rootLogger;
  private Enumeration<Appender> savedRootAppenders;
  private Level savedRootLevel;

  @BeforeClass
  @SuppressWarnings("unchecked")
  public void setUp() {
    this.rootLogger = org.apache.log4j.Logger.getRootLogger();
    this.savedRootLevel = this.rootLogger.getLevel();
    this.savedRootAppenders = this.rootLogger.getAllAppenders();
  }

  @AfterClass
  public void tearDown() {
    this.rootLogger.removeAllAppenders();
    this.rootLogger.setLevel(this.savedRootLevel);
    while (this.savedRootAppenders.hasMoreElements()) {
      this.rootLogger.addAppender(this.savedRootAppenders.nextElement());
    }
  }

  @Test
  public void testChangeLoggingLevel() {
    Logger log = LoggerFactory.getLogger(TestLog4jUtils.class.getName() + ".testChangeLoggingLevel");
    Log4jUtils.changeLoggingLevel(log, "DEBUG");
    Assert.assertEquals(org.apache.log4j.Logger.getLogger(log.getName()).getLevel(), Level.DEBUG);
    Log4jUtils.changeLoggingLevel(log, "ERROR");
    Assert.assertEquals(org.apache.log4j.Logger.getLogger(log.getName()).getLevel(), Level.ERROR);
  }

  @Test(dependsOnMethods="testChangeLoggingLevel")
  public void testConfigureLoggingForTest() {
    //count log files from previous runs
    File tmpDir = new File("/tmp");
    String[] logFiles = tmpDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.contains(TestLog4jUtils.class.getSimpleName());
      }
    });
    int initialLogFileNum = null == logFiles ? 0 : logFiles.length;

    Log4jUtils.configureLoggingForTest(TestLog4jUtils.class, "DEBUG", true);
    Assert.assertEquals(rootLogger.getLevel(), Level.DEBUG);
    @SuppressWarnings("unchecked")
    Enumeration<Appender> allAppenders = rootLogger.getAllAppenders();
    boolean hasFileAppender = false;
    boolean hasConsoleAppender = false;
    FileAppender fileAppender = null;
    this.rootLogger.debug("Starting appender asserts");
    while (allAppenders.hasMoreElements()) {
      Appender appender = allAppenders.nextElement();
      if (appender instanceof ConsoleAppender) {
        hasConsoleAppender = true;
      } else if (appender instanceof FileAppender) {
        hasFileAppender = true;
        fileAppender = (FileAppender)appender;
      }
      this.rootLogger.info("hasFileAppender=" + hasFileAppender);
      this.rootLogger.info("hasConsoleAppender=" + hasConsoleAppender);
    }
    Assert.assertTrue(hasFileAppender);
    Assert.assertTrue(hasConsoleAppender);
    this.rootLogger.debug("Done with appender asserts");

    this.rootLogger.debug("Check for log file");
    this.rootLogger.info("initialLogFileNum=" + initialLogFileNum);
    fileAppender.close();
    logFiles = tmpDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.contains(TestLog4jUtils.class.getSimpleName());
      }
    });
    int finalLogFileNum = null == logFiles ? 0 : logFiles.length;
    Assert.assertEquals(finalLogFileNum, initialLogFileNum + 1);
  }

}
