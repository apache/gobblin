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
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;


/**
 * A SSL socket wrapper used to create sockets with a {@link: javax.net.ssl.TrustManager}
 */
public class TrustManagerSocketFactory extends SSLSocketFactory {
  private SSLSocketFactory _sslSocketFactory;

  public TrustManagerSocketFactory() {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, new TrustManager[]{new DummyTrustManager()}, new SecureRandom());
      _sslSocketFactory = ctx.getSocketFactory();
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
    }
  }

  public static SocketFactory getDefault() {
    return new TrustManagerSocketFactory();
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
    return _sslSocketFactory.createSocket(socket, host, port, autoClose);
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return _sslSocketFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return _sslSocketFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    return _sslSocketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return _sslSocketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException, UnknownHostException {
    return _sslSocketFactory.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return _sslSocketFactory.createSocket(address, port, localAddress, localPort);
  }
}
