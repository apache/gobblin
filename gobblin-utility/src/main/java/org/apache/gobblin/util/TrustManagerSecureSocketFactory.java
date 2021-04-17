/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;


public class TrustManagerSecureSocketFactory extends SSLSocketFactory {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TrustManagerSecureSocketFactory.class);

  private SSLSocketFactory sslSocketFactory;
  private String[] ciphers;
  private String protocol;

  public TrustManagerSecureSocketFactory() {
    this(null, null);
  }

  public TrustManagerSecureSocketFactory(String[] ciphers, String protocol) {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      this.sslSocketFactory = ctx.getSocketFactory();
      this.ciphers = (null == ciphers) ? sslSocketFactory.getSupportedCipherSuites() : ciphers;
      this.protocol = (null == protocol) ? ctx.getProtocol() : protocol;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Issue in initializing TrustManagerSecureSocketFactory.", e);
    }
  }

  public String[] getDefaultCipherSuites() {
    return sslSocketFactory.getDefaultCipherSuites();
  }

  public String[] getSupportedCipherSuites() {
    return sslSocketFactory.getSupportedCipherSuites();
  }

  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException  {
    return enableCipherSuites(sslSocketFactory.createSocket(s, host, port, autoClose),
        new Object[]{host, port});
  }

  public Socket createSocket(String host, int port) throws IOException {
    return enableCipherSuites(sslSocketFactory.createSocket(host, port),
        new Object[]{host, port});
  }

  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return enableCipherSuites(sslSocketFactory.createSocket(host, port, localHost, localPort),
        new Object[]{host, port});
  }

  public Socket createSocket(InetAddress host, int port) throws IOException {
    return enableCipherSuites(sslSocketFactory.createSocket(host, port),
        new Object[]{host, port});
  }

  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return enableCipherSuites(sslSocketFactory.createSocket(address, port, localAddress, localPort),
        new Object[]{address, port});
  }

  private Socket enableCipherSuites(Socket s, Object[] logParams) {
    SSLSocket socket = (SSLSocket)s;

    if (socket == null) {
      LOG.warn("PROBLEM_CREATING_OUTBOUND_REQUEST_SOCKET", logParams);
      return null;
    }

    if (protocol != null) {
      String[] p = findProtocols(protocol, socket.getSupportedProtocols());
      if (p != null) {
        socket.setEnabledProtocols(p);
      }
    }
    if (ciphers != null) {
      socket.setEnabledCipherSuites(ciphers);
    }

    return socket;
  }
  private String[] findProtocols(String p, String[] options) {
    List<String> list = new ArrayList<>();
    for (String s : options) {
      if (s.equals(p)) {
        return new String[] {p};
      } else if (s.startsWith(p)) {
        list.add(s);
      }
    }
    if (list.isEmpty()) {
      return null;
    }
    return list.toArray(new String[0]);
  }
}