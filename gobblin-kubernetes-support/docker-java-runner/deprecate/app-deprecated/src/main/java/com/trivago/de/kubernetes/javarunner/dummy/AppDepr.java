package com.trivago.de.kubernetes.javarunner.dummy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Deprecated
public final class AppDepr {

    private static final String KLASS = "com.trivago.de.kubernetes.javarunner.dummy.Dummy";
    private static final String METHOD = "main";
    private static final String JAR = "/a/dummy.jar";

    private static final String HOST = "0.0.0.0";
    private static final int PORT = 8080;
    private static final int BACKLOG = 1;

    private static final String WORKSPACE = "/a";

    private static final String HEADER__JAR = "x-jar";
    private static final String HEADER__KLASS = "x-java-klass";
    private static final String HEADER__METHOD = "x-java-method";

    public static void main(String[] args) throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(HOST, PORT), BACKLOG);
        server.createContext("/", exchange -> {
            switch (exchange.getRequestMethod()) {
                case "PUT":
                    try {
                        put(exchange);
                    }
                    catch (final Exception e) {
                        write("error: " + e.getMessage(), exchange, 500);
                    }
                    break;

                case "POST":
                    try {
                        post(exchange);
                    }
                    catch (final Exception e) {
                        throw new IOException(e);
                    }
                    break;

                default:
                    throw new IOException("bad request method: " + exchange.getRequestMethod());
            }
        });
        server.start();

        run(JAR, KLASS, METHOD);
    }

    private static void put(final HttpExchange exchange) throws Exception {
        final String requestURI = exchange.getRequestURI().toString();
        if (!requestURI.startsWith("/") || requestURI.chars().filter(ch -> ch == '/').count() != 1)
            throw new IOException("bad request uri: " + requestURI);

        final File file = new File(WORKSPACE + requestURI);
        if (file.exists())
            throw new IOException("already exists: " + file);

        try (final FileOutputStream out = new FileOutputStream(file);
             final BufferedInputStream in = new BufferedInputStream(exchange.getRequestBody())) {
            final byte[] buf = new byte[8 * 1024];
            int n;
            while ((n = in.read(buf)) > 0)
                out.write(buf, 0, n);
        }

        exchange.close();
    }

    private static void post(final HttpExchange exchange) throws Exception {
        final String jar = exchange.getRequestHeaders().getFirst(HEADER__JAR);
        final String klass = exchange.getRequestHeaders().getFirst(HEADER__KLASS);
        final String method = exchange.getRequestHeaders().getFirst(HEADER__METHOD);

        if (jar == null || jar.isEmpty())
            throw new IOException("header missing: " + HEADER__JAR);
        if (klass == null || klass.isEmpty())
            throw new IOException("header missing: " + HEADER__KLASS);
        if (method == null || method.isEmpty())
            throw new IOException("header missing: " + HEADER__METHOD);

        write("starting...", exchange, 200);

        run(jar, klass, method);
    }

    private static void write(final String response,
                              final HttpExchange exchange,
                              final int httpCode) {
        final byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        try {
            exchange.sendResponseHeaders(httpCode, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
            exchange.getResponseBody().flush();
            exchange.getResponseBody().close();
        }
        catch (final IOException e) {
            System.out.println("error writing to client: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void run(final String jar,
                            final String klass,
                            final String method) throws Exception {
        final File jarFile = new File(jar);
        final URL url = jarFile.toURI().toURL();

        final TrvClassLoader cl = TrvClassLoader.getTrvClassLoader();
        cl.add(url);

        final Class<?> k = Class.forName(klass, true, cl);
        final Method m = k.getMethod(method);
        m.setAccessible(true);
        m.invoke(null);
    }

}
