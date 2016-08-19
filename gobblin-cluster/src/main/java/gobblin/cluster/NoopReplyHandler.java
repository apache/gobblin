package gobblin.cluster;

import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that handles Helix messaging response via no-op.
 *
 * @author Abhishek Tiwari
 */
public class NoopReplyHandler extends AsyncCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoopReplyHandler.class);

  private String bootstrapUrl;
  private String bootstrapTime;

  public NoopReplyHandler() {
  }

  public void onTimeOut() {
    LOGGER.error("Timed out");
  }

  public void onReplyMessage(Message message) {
    LOGGER.info("Received reply: " + message);
  }
}
