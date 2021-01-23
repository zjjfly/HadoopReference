/bin/env bash
# add helm repo
helm repo add stable https://charts.helm.sh/stable
helm repo add incubator https://charts.helm.sh/incubator

# install hadoop cluster
helm install -name hadoop \
  --set yarn.nodeManager.resources.limits.memory=4096Mi \
  --set yarn.nodeManager.replicas=1 \
  stable/hadoop

# switch to ame namespace
kubectl config set-context "$(kubectl config current-context)" --namespace ame

#create a service to expose hdfs name node port
kubectl apply -f hdfs-nn-svc.yaml

# edit service to expose yarn ui port(change service type to NodePort)
kubectl edit svc hadoop-hadoop-yarn-ui
