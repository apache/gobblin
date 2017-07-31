package org.apache.gobblin.util;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class HostUtils {
  public static String getHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Error determining hostname", e);
    }
  }

  public static String getPrincipalUsingHostname(String name, String realm) {
    return name + "/" + getHostName() + "@" + realm;
  }
}
