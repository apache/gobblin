/* (c) 2015 NerdWallet All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/
package gobblin.admin;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import gobblin.configuration.ConfigurationKeys;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;

/**
 * Serves the admin UI interface using embedded Jetty.
 */
public class AdminWebServer extends AbstractIdleService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminWebServer.class);

    private final URI restServerUri;
    private final URI serverUri;
    protected Server server;

    public AdminWebServer(Properties properties, URI restServerUri) {
        Preconditions.checkNotNull(properties);
        Preconditions.checkNotNull(restServerUri);

        this.restServerUri = restServerUri;
        int port = getPort(properties);
        serverUri = URI.create(String.format("http://%s:%d", getHost(properties), port));
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting the admin web server");

        server = new Server(new InetSocketAddress(serverUri.getHost(), serverUri.getPort()));

        HandlerCollection handlerCollection = new HandlerCollection();

        handlerCollection.addHandler(buildSettingsHandler());
        handlerCollection.addHandler(buildStaticResourceHandler());

        server.setHandler(handlerCollection);
        server.start();
    }

    private Handler buildSettingsHandler() {
        final String responseTemplate =
                "var Gobblin = window.Gobblin || {};" +
                "Gobblin.settings = {restServerUrl:\"%s\"}";

        return new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
                if (request.getRequestURI().equals("/js/settings.js")) {
                    response.setContentType("application/javascript");
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.getWriter().println(String.format(responseTemplate, restServerUri.toString()));
                    baseRequest.setHandled(true);
                }
            }
        };
    }

    private ResourceHandler buildStaticResourceHandler() {
        ResourceHandler staticResourceHandler = new ResourceHandler();
        staticResourceHandler.setDirectoriesListed(true);
        staticResourceHandler.setWelcomeFiles(new String[]{"index.html"});

        String staticDir = getClass().getClassLoader().getResource("static").toExternalForm();

        staticResourceHandler.setResourceBase(staticDir);
        return staticResourceHandler;
    }

    @Override
    protected void shutDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    private static int getPort(Properties properties) {
        return Integer.parseInt(properties.getProperty(
                ConfigurationKeys.ADMIN_SERVER_PORT_KEY,
                ConfigurationKeys.DEFAULT_ADMIN_SERVER_PORT));
    }

    private static String getHost(Properties properties) {
        return properties.getProperty(
                ConfigurationKeys.ADMIN_SERVER_HOST_KEY,
                ConfigurationKeys.DEFAULT_ADMIN_SERVER_HOST);
    }
}
