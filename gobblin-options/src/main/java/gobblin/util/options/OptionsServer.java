package gobblin.util.options;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;

import com.google.common.base.Joiner;
import com.google.gson.Gson;


/**
 * Created by ibuenros on 3/5/16.
 */
@Slf4j
public class OptionsServer {

  private static Joiner joiner = Joiner.on("");
  private static Gson gson = new Gson();

  public static void main(String[] args) throws Exception
  {
    Server server = new Server(9000);

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    resourceHandler.setResourceBase("gobblin-utility/src/main/resources/web");
    resourceHandler.setWelcomeFiles(new String[]{"index.html"});

    ContextHandler context = new ContextHandler();
    context.setContextPath("/options");
    context.setHandler(new OptionsHandler());

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{ context, resourceHandler, new DefaultHandler()});
    server.setHandler(handlers);

    server.start();
    server.join();
  }

  public static class OptionsHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
      try {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);

        BufferedReader reader = request.getReader();
        String data = joiner.join(IOUtils.readLines(reader));

        OptionsRequest optionsRequest = gson.fromJson(data, OptionsRequest.class);

        log.info(optionsRequest.toString());

        Class<?> klazz = Class.forName(optionsRequest.theClass);
        Properties properties = new Properties();
        properties.putAll(optionsRequest.properties);

        String options = new OptionFinder().getJsonOptionsForClass(klazz, properties);

        response.getWriter().println(options);
      } catch (ReflectiveOperationException roe) {
        throw new IOException(roe);
      }
    }
  }

  @RequiredArgsConstructor
  @ToString
  public static class OptionsRequest {
    private final String theClass;
    private final Map<String, String> properties;
  }

}
