package com.trivago.de.kubernetes.javarunner;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;

public final class App {

    private static final String HOST = "0.0.0.0";
    private static final int PORT = 8080;
    private static final int BACKLOG = 1;

    private static final String HEADER__KLASS = "x-java-klass";
    private static final String HTTP_METHOD = "POST";

    private final HttpServer server;


    public static void main(final String... args) throws Exception {
        final App app = new App();
        app.start();
    }


    private App() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(HOST, PORT), BACKLOG);
        server.createContext("/", this::handle);
    }

    private void start() {
        System.out.println("classpath ===>");
        final ClassLoader cl = ClassLoader.getSystemClassLoader();
        for (final URL url : ((URLClassLoader) cl).getURLs())
            System.out.println(url.getFile());

        this.server.start();
        System.out.println("serving on: " + HOST + ":" + PORT);
    }

    private void stop() {
        this.server.stop(1);
    }


    private void handle(final HttpExchange exchange) {
        if (!HTTP_METHOD.equals(exchange.getRequestMethod())) {
            writeAndClose("bad request method, expecting " + HTTP_METHOD
                    + ", got=" + exchange.getRequestMethod(), exchange, 400);
            return;
        }


        final String body;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             final BufferedInputStream in = new BufferedInputStream(exchange.getRequestBody())) {
            final byte[] buf = new byte[8 * 1024];
            int n;
            while ((n = in.read(buf)) > 0)
                out.write(buf, 0, n);
            body = new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
        catch (final IOException e) {
            e.printStackTrace();
            writeAndClose("failed to read body", exchange, 400);
            return;
        }

        final String klass = exchange.getRequestHeaders().getFirst(HEADER__KLASS);
        if (klass == null || klass.isEmpty()) {
            writeAndClose("missing required header=" +
                    HEADER__KLASS, exchange, 400);
            return;
        }
        if (App.class.getName().equals(klass)) {
            writeAndClose("can not start self", exchange, 400);
            return;
        }

        final Method method;
        try {
            final Class<?> k = Class.forName(klass);
            method = k.getMethod("main", String[].class);
            method.setAccessible(true);
        }
        catch (final ClassNotFoundException | NoSuchMethodException e) {
            writeAndClose("error locating the requested class/method: "
                            + e.getMessage(),
                    exchange, 400);
            return;
        }

        writeAndClose("calling target method. result WILL NOT be reported,"
                        + " even on error. please check the logs instead",
                exchange, 200);
        this.stop();

        System.out.println("server stopped, calling client code");

        try {
            final String[] split = body.split("\n");
            method.invoke(null, (Object) split);
        }
        catch (final InvocationTargetException e) {
            if (e.getCause() != null)
                e.getCause().printStackTrace();
            else
                e.printStackTrace();
            System.out.println("failed");
            System.exit(1);
        }
        catch (final Exception e) {
            e.printStackTrace();
            System.out.println("failed");
            System.exit(1);
        }

        System.exit(0);
    }

    private static void writeAndClose(final String response,
                                      final HttpExchange exchange,
                                      final int httpCode) {
        System.out.println("saying to client, code=" + httpCode + ", message=" + response);
        final byte[] responseBytes = (response + "\n").getBytes(StandardCharsets.UTF_8);
        try {
            exchange.sendResponseHeaders(httpCode, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
            exchange.getResponseBody().flush();
        }
        catch (final IOException e) {
            System.out.println("error writing to client: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            exchange.close();
        }
    }


}
