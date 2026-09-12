from fastmcp import FastMCP
from kubernetes import client, config
from typing import Dict, Any, Optional

mcp = FastMCP("GKE-Cluster-Inspector")


def _init_k8s():
    """Load local kubeconfig (configured for GKE via gcloud)."""
    try:
        config.load_kube_config()
    except Exception as e:
        raise RuntimeError(f"Failed to load kubeconfig: {str(e)}")


@mcp.tool()
def get_pod_status_summary(namespace: str = "default") -> Dict[str, Any]:
    _init_k8s()
    v1 = client.CoreV1Api()

    pods = v1.list_namespaced_pod(namespace=namespace)
    summary = {
        "namespace": namespace,
        "total_pods": len(pods.items),
        "running": 0,
        "pending": 0,
        "failed": 0,
        "succeeded": 0,
        "containers_restarting": 0,
        "pod_details": [],
    }

    for pod in pods.items:
        phase = pod.status.phase
        if phase == "Running":
            summary["running"] += 1
        elif phase == "Pending":
            summary["pending"] += 1
        elif phase == "Failed":
            summary["failed"] += 1
        elif phase == "Succeeded":
            summary["succeeded"] += 1

        restarts = 0
        container_statuses = pod.status.container_statuses or []
        for cs in container_statuses:
            restarts += cs.restart_count

        if restarts > 0:
            summary["containers_restarting"] += 1

        summary["pod_details"].append(
            {
                "name": pod.metadata.name,
                "phase": phase,
                "restarts": restarts,
                "pod_ip": pod.status.pod_ip,
            }
        )

    return summary


@mcp.tool()
def get_node_status() -> Dict[str, Any]:
    _init_k8s()
    v1 = client.CoreV1Api()
    nodes = v1.list_node()

    node_list = []
    for node in nodes.items:
        conditions = {c.type: c.status for c in node.status.conditions}
        node_list.append(
            {
                "name": node.metadata.name,
                "ready": conditions.get("Ready") == "True",
                "capacity": node.status.capacity,
            }
        )

    return {"total_nodes": len(node_list), "nodes": node_list}


@mcp.tool()
def get_deployment_resources(
    deployment_name: str, namespace: str = "default"
) -> Dict[str, Any]:
    """
    Fetch the current CPU and Memory requests and limits for containers in a Deployment.
    """
    _init_k8s()
    apps_v1 = client.AppsV1Api()

    dep = apps_v1.read_namespaced_deployment(name=deployment_name, namespace=namespace)
    containers = dep.spec.template.spec.containers

    container_info = []
    for c in containers:
        res = c.resources
        container_info.append(
            {
                "name": c.name,
                "requests": res.requests if res else None,
                "limits": res.limits if res else None,
            }
        )

    return {
        "deployment": deployment_name,
        "namespace": namespace,
        "containers": container_info,
    }


@mcp.tool()
def optimize_deployment_resources(
    deployment_name: str,
    container_name: str,
    cpu_request: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    memory_request: Optional[str] = None,
    memory_limit: Optional[str] = None,
    namespace: str = "default",
    dry_run: bool = True,
) -> Dict[str, Any]:

    _init_k8s()
    apps_v1 = client.AppsV1Api()

    dep = apps_v1.read_namespaced_deployment(name=deployment_name, namespace=namespace)
    target_container = None

    for c in dep.spec.template.spec.containers:
        if c.name == container_name:
            target_container = c
            break

    if not target_container:
        return {
            "error": f"Container '{container_name}' not found in deployment '{deployment_name}'"
        }

    current_res = target_container.resources
    requests = (
        current_res.requests.copy() if (current_res and current_res.requests) else {}
    )
    limits = current_res.limits.copy() if (current_res and current_res.limits) else {}

    MAX_ALLOWED_CPU = 2.0  # e.g., 2000m
    MAX_ALLOWED_MEM_GI = 4.0  # e.g., 4Gi

    if cpu_request:
        requests["cpu"] = cpu_request
    if memory_request:
        requests["memory"] = memory_request
    if cpu_limit:
        limits["cpu"] = cpu_limit
    if memory_limit:
        limits["memory"] = memory_limit

    target_container.resources = client.V1ResourceRequirements(
        requests=requests, limits=limits
    )

    if dry_run:
        return {
            "status": "dry_run_success",
            "message": "Previewing updates (no changes applied to cluster).",
            "proposed_requests": requests,
            "proposed_limits": limits,
        }

    updated_dep = apps_v1.patch_namespaced_deployment(
        name=deployment_name, namespace=namespace, body=dep
    )

    return {
        "status": "applied",
        "message": f"Successfully updated resources for {deployment_name}/{container_name}",
        "active_requests": requests,
        "active_limits": limits,
    }


app = mcp.http_app()
