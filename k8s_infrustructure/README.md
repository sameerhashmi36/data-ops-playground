# Kubernetes Infrastructure Learning Notes

## Overview

This folder contains learning exercises used to understand the fundamental concepts of Kubernetes. The goal is not only to deploy containers, but also to understand how Kubernetes manages applications, monitors their health, and automatically recovers from failures.

---

## Installing kubectl on Ubuntu

Install the Kubernetes command-line tool (`kubectl`):

```bash
# Download the latest stable kubectl binary
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

# Make the binary executable and move it to the system path
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl

# Verify installation
kubectl version --client
```

`kubectl` is the primary tool used to communicate with a Kubernetes cluster. Every deployment, update, inspection, and deletion operation is typically performed through this command.

---

## Understanding the Kubernetes Workflow

### Step 1: Create a Deployment

A Deployment describes the desired state of an application.

Instead of manually starting containers, Kubernetes is given a configuration file that defines:

- Which container image should run
- How many copies (replicas) should exist
- Labels used for identification
- Health checks and other settings

The configuration is stored in a YAML file.

Apply the configuration:

```bash
kubectl apply -f <file-name>.yaml
```

After the deployment is created, Kubernetes begins working to achieve the desired state.

---

### Step 2: Kubernetes Creates Pods

A Pod is the smallest deployable unit in Kubernetes.

When a Deployment is applied:

1. Kubernetes reads the configuration.
2. A ReplicaSet is created.
3. The ReplicaSet creates the required Pods.
4. Containers start running inside those Pods.

View running Pods:

```bash
kubectl get pods
```

Watch changes in real time:

```bash
kubectl get pods -w
```

---

### Step 3: Desired State Management

One of the most important Kubernetes concepts is the desired state.

For example:

- Desired replicas = 2
- Current replicas = 2

Everything is healthy.

If one Pod disappears:

- Desired replicas = 2
- Current replicas = 1

Kubernetes detects the difference and immediately creates another Pod.

This process happens automatically without manual intervention.

---

### Step 4: Self-Healing

Kubernetes continuously monitors running applications.

If a Pod:

- Crashes
- Stops responding
- Is deleted manually
- Fails a health check

Kubernetes automatically attempts to restore the desired state.

This behavior is known as **self-healing**.

A deleted Pod is replaced automatically because the Deployment still requires the specified number of replicas.

---

### Step 5: Health Checks

Applications can provide health information through probes.

A liveness probe allows Kubernetes to determine whether a container is still functioning correctly.

If a container becomes unhealthy:

1. Kubernetes marks it as failed.
2. The container is restarted.
3. If necessary, a new Pod is created.

This ensures applications remain available even when failures occur.

---

### Step 6: Handling Crashes

Applications sometimes terminate unexpectedly.

When a container exits with an error:

1. Kubernetes detects the failure.
2. The Pod status changes.
3. Restart attempts begin automatically.
4. Restart counts are recorded for troubleshooting.

Useful commands:

```bash
kubectl logs <pod-name> --previous
```

View logs from a previous crashed container.

```bash
kubectl describe pod <pod-name>
```

View detailed information, events, restart counts, and failure reasons.

---

### Step 7: Observing Kubernetes Recovery

While monitoring Pods:

```bash
kubectl get pods -w
```

Several behaviors can be observed:

- Pods entering Running state
- Pods restarting after failures
- New Pods appearing after deletions
- Kubernetes continuously attempting recovery

This demonstrates how Kubernetes focuses on maintaining application availability.

---

### Step 8: Removing Resources

Resources created from a YAML file can be removed using:

```bash
kubectl delete -f <file-name>.yaml
```

When a Deployment is deleted:

1. The Deployment is removed.
2. The ReplicaSet is removed.
3. The Pods managed by that Deployment are removed.

Since the desired state no longer exists, Kubernetes stops recreating the Pods.

---

## Summary

### Cluster 
A collection of machines managed by Kubernetes.

### Node
A machine that runs workloads.

### Pod
The smallest deployable unit in Kubernetes. A Pod contains one or more containers.

### Deployment
A controller that manages Pods and maintains the desired state.

### Replica
An additional copy of an application Pod.

### Self-Healing
Automatic recovery from crashes, failures, or deletions.

### Liveness Probe
A health check used to determine whether a container is still functioning correctly.

### Desired State
The target state defined in the YAML configuration. Kubernetes continuously works to keep the actual state equal to the desired state.

---

Kubernetes is a container orchestration platform designed to automate application management.

The basic workflow is:

1. Install kubectl.
2. Create a Deployment YAML file.
3. Apply the configuration.
4. Kubernetes creates Pods.
5. Kubernetes monitors application health.
6. Failed Pods are restarted automatically.
7. Deleted Pods are recreated automatically.
8. Resources can be removed when no longer needed.

The most important lesson is that Kubernetes is constantly working to maintain the desired state defined in the configuration files.
