package gobblin.tunnel;

class Config {
  public static final int PROXY_CONNECT_TIMEOUT_MS = 5000;
  private final String remoteHost;
  private final int remotePort;
  private final String proxyHost;
  private final int proxyPort;

  public Config(String remoteHost, int remotePort, String proxyHost, int proxyPort) {
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
  }

  public String getRemoteHost() {
    return remoteHost;
  }

  public int getRemotePort() {
    return remotePort;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }
}
