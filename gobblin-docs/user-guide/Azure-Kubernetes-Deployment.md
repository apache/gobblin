# GaaS on Azure Deployment Steps

## Create Azure Container Registry [Optional]

1\) Log into Azure Container Registry

```bash
$ az acr login --name gobblintest
```

2\) Tag docker images to container registry

```bash
$ docker tag <gaas_image_id> gobblintest.azurecr.io/gobblin-service
$ docker tag <standalone_image_id> gobblintest.azurecr.io/gobblin-standalone
```

3\) Push the images

```bash
$ docker push gobblintest.azurecr.io/gobblin-service
$ docker push gobblintest.azurecr.io/gobblin-standalone
```

The images should now be hosted on azure with the tag:latest

## Deploy the base K8s cluster

1\) Create a resource group on Azure

2\) Create a cluster and deploy it onto the resource group

```bash
az aks create --resource-group <resource_group_name> --name GaaS-cluster-test --node-count 1 --enable-addons monitoring --generate-ssh-keys
```

3\) Switch kubectl to use azure

4\) Check status of cluster

```bash
$ kubectl get pods
```

## Install the nginx ingress to connect to the Azure Cluster

1\) Install helm if you don't currently have it

```bash
brew install helm
helm init
```

2\) Deploy the nginx helm chart to create the ingress

```bash
helm install stable/nginx-ingress
```

If this is the first time deploying helm (v2.0), you will need to set up the tiller, which is a helm serviceaccount with sudo permissions that lives inside of the cluster. Otherwise you'll run into this [issue](https://github.com/helm/helm/issues/2224).

> Error: configmaps is forbidden: User "system:serviceaccount:kube-system:default" cannot list configmaps in the namespace "kube-system"

To set up the tiller \(steps are also found in the issue link\)

```bash
kubectl create serviceaccount --namespace kube-system tiller
kubectl create clusterrolebinding tiller-cluster-rule --clusterrole=cluster-admin --serviceaccount=kube-system:tiller
kubectl edit deploy --namespace kube-system tiller-deploy #and add the line serviceAccount: tiller to spec/template/spec
```

3\) Deploy the ingress controller in `gobblin-kubernetes/gobblin-service/azure-cluster`

4\) Run `kubectl get services`, and the output should look something like this:

```text
gaas-svc                                        ClusterIP      10.0.176.58    <none>           6956/TCP                     16h
honorary-possum-nginx-ingress-controller        LoadBalancer   10.0.182.255   <EXTERNAL_IP>    80:30488/TCP,443:31835/TCP   6m13s
honorary-possum-nginx-ingress-default-backend   ClusterIP      10.0.236.153   <none>           80/TCP                       6m13s
kubernetes                                      ClusterIP      10.0.0.1       <none>           443/TCP                      10d
```

5\) Send a request to the IP for the `honorary-possum-nginx-ingress-controller`
