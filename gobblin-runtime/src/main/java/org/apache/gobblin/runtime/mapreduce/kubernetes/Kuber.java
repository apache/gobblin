package org.apache.gobblin.runtime.mapreduce.kubernetes;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({"unused", "UnusedReturnValue"})
public final class Kuber {

    private static final Joiner JOINER = Joiner.on('/');

    private static final String IMAGE = "k3d-default-registry.localhost:5000/trv-local-java-runner:v0.2";

    private Kuber() {

    }

    private static final Object INIT_LOCK = new Object();
    private static volatile boolean init = false;

    private static CoreV1Api getApi() throws IOException {
        synchronized (INIT_LOCK) {
            if (!init) {
                final ApiClient client = Config.defaultClient();
                Configuration.setDefaultApiClient(client);
                init = true;
            }
        }

        return new CoreV1Api();
    }


    public static V1PodList lsPods(final boolean printToStdOut) throws IOException, ApiException {
        final V1PodList v1PodList = getApi().listPodForAllNamespaces(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        if (printToStdOut)
            for (final V1Pod item : v1PodList.getItems()) {
                System.out.println(item.getMetadata());
                if (item.getMetadata() != null)
                    System.out.println(item.getMetadata().getName());
            }

        return v1PodList;
    }

    public static V1Pod getPod() throws IOException, ApiException {
        final V1PodList v1PodList = getApi().listPodForAllNamespaces(
                null,
                null,
                null,
                "gobblin=runner",
                null,
                null,
                null,
                null,
                null,
                null
        );

        final List<V1Pod> items = v1PodList.getItems();
        if (items.size() == 0)
            throw new IOException("no pod found");
        if (items.size() > 1)
            throw new IOException("too many pod found: " + items.size());
        return items.iterator().next();
    }

    public static V1Pod start(final String jobStuffLocation) throws IOException, ApiException {
        final String NAMESPACE = "default";
        final List<String> cmd = ImmutableList.of(
                "/start.sh",
                JOINER.join("/workspace", jobStuffLocation, "jars"),
                KubeTaskRunner.class.getName(),
                JOINER.join("/workspace", jobStuffLocation)
        );

        final V1Pod body = new V1Pod()
                .apiVersion("v1")
                .kind("Pod")
                .spec(
                        new V1PodSpec()
                                .restartPolicy("Never")
                                .addVolumesItem(
                                        new V1Volume()
                                                .name("workspace")
                                                .persistentVolumeClaim(
                                                        new V1PersistentVolumeClaimVolumeSource()
                                                                .claimName("workspace")
                                                )
                                )
                                .addContainersItem(
                                        new V1Container()
                                                .name("gobblin-runner")
                                                .image(IMAGE)
                                                .imagePullPolicy("Always")
                                                .command(cmd)
                                                .addVolumeMountsItem(
                                                        new V1VolumeMount()
                                                                .mountPath("/workspace")
                                                                .name("workspace")
                                                )
                                )
                );
        body.setMetadata(new V1ObjectMeta().name("gobblin-runner").putLabelsItem("gobblin", "runner"));

        final V1Pod namespacedPod = getApi().createNamespacedPod(NAMESPACE, body, null, null, null);
        return namespacedPod;
    }

    public static V1Pod stop() throws IOException, ApiException {
        final String NAMESPACE = "default";

        final String name = Objects.requireNonNull(getPod().getMetadata(), "metadata")
                                   .getName();

        final V1Pod v1Pod = getApi().deleteNamespacedPod(
                name,
                NAMESPACE,
                null,
                null,
                3,
                null,
                null,
                null
        );
        return v1Pod;
    }

}
