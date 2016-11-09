package gobblin.util;

/**
 * A utility class for the application launcher.
 */
public class ApplicationLauncherUtils {

  /**
   * Create a new app ID.
   *
   * @param appName application name
   * @return new app ID
   */
  public static String newAppId(String appName) {
    String appIdSuffix = String.format("%s_%d", appName, System.currentTimeMillis());
    return "app_" + appIdSuffix;
  }
}
