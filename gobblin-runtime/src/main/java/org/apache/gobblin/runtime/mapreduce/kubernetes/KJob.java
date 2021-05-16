package org.apache.gobblin.runtime.mapreduce.kubernetes;

import io.kubernetes.client.openapi.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class KJob {

    private static final Logger LOG = LoggerFactory.getLogger(KJob.class);

    private final String jobName;


    private KJob(final String jobName) {
        this.jobName = Objects.requireNonNull(jobName);
    }


    public String getJobName() {
        return this.jobName;
    }


    public void kill() throws IOException {
        try {
            Kuber.stop();
        }
        catch (final ApiException e) {
            throw new IOException(e);
        }
    }

    public void submit(final String jobStuffLocation) throws IOException {
        try {
            Kuber.start(jobStuffLocation);
        }
        catch (final ApiException e) {
            throw new IOException(e);
        }
    }

    public void waitForCompletion() throws IOException {
        try {
            while (!isComplete())
                Thread.sleep(1000L);
        }
        catch (final InterruptedException e) {
            throw new IOException(e);
        }
    }


    public boolean isSuccessful() throws IOException {
        return this.getPhase().equals("succeeded");
    }

    public boolean isComplete() throws IOException {
        final String phase = this.getPhase();
        return phase.equals("succeeded") || phase.equals("failed");
    }


    private String getPhase() throws IOException {
        try {
            @SuppressWarnings("ConstantConditions")
            final String phase = Kuber.getPod()
                                      .getStatus()
                                      .getPhase()
                                      .toLowerCase();
            if ("failed".equals(phase))
                LOG.error("kuber job has entered failed state");
            return phase;
        }
        catch (final ApiException e) {
            throw new IOException(e);
        }
    }


    public static KJob getInstance(final String jobName) {
        return new KJob(jobName);
    }

}
