import functools
from typing import Any, Dict, Optional
from fastmcp import FastMCP
from kubernetes import client, config

mcp = FastMCP("GKE-Cluster-Inspector")


@functools.lru_cache(maxsize=1)
def _init_k8s():
    """Load K8s config once and cache initialization."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except Exception as e:
            raise RuntimeError(f"Failed to load any Kubernetes configuration: {str(e)}")


# ------------------------------------------------------------------------------
# CORE OBSERVABILITY TOOLS
# ------------------------------------------------------------------------------


@mcp.tool()
def get_pod_status_summary(namespace: str = "default") -> Dict[str, Any]:
    """Get high-level status summary for all pods in a namespace."""
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
        if phase in summary:
            summary[phase.lower()] += 1

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
def get_pod_logs(
    pod_name: str,
    namespace: str = "default",
    container: Optional[str] = None,
    tail_lines: int = 100,
    previous: bool = False,
) -> Dict[str, Any]:
    """
    Fetch recent logs for a pod. Essential for troubleshooting crashed/restarting containers.
    """
    _init_k8s()
    v1 = client.CoreV1Api()
    try:
        logs = v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container,
            tail_lines=tail_lines,
            previous=previous,
        )
        return {
            "pod_name": pod_name,
            "namespace": namespace,
            "container": container,
            "logs": logs,
        }
    except Exception as e:
        return {"error": f"Failed to fetch logs: {str(e)}"}


@mcp.tool()
def get_namespace_events(namespace: str = "default", limit: int = 20) -> Dict[str, Any]:
    """
    Fetch recent warning and error events in a namespace to diagnose crashloops or scheduling issues.
    """
    _init_k8s()
    v1 = client.CoreV1Api()
    try:
        events = v1.list_namespaced_event(namespace=namespace)
        sorted_events = sorted(
            events.items, key=lambda e: e.last_timestamp or e.event_time, reverse=True
        )[:limit]

        event_list = [
            {
                "type": e.type,
                "reason": e.reason,
                "message": e.message,
                "object": f"{e.involved_object.kind}/{e.involved_object.name}",
                "last_timestamp": str(e.last_timestamp),
            }
            for e in sorted_events
        ]
        return {"namespace": namespace, "events": event_list}
    except Exception as e:
        return {"error": f"Failed to fetch events: {str(e)}"}


@mcp.tool()
def get_node_status() -> Dict[str, Any]:
    """List cluster nodes along with readiness and capacity metrics."""
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
                "allocatable": node.status.allocatable,
            }
        )

    return {"total_nodes": len(node_list), "nodes": node_list}


# ------------------------------------------------------------------------------
# DEPLOYMENT & SCALING REMEDIATION TOOLS
# ------------------------------------------------------------------------------


@mcp.tool()
def get_deployment_resources(
    deployment_name: str, namespace: str = "default"
) -> Dict[str, Any]:
    """Fetch current CPU and Memory requests and limits for containers in a Deployment."""
    _init_k8s()
    apps_v1 = client.AppsV1Api()

    try:
        dep = apps_v1.read_namespaced_deployment(
            name=deployment_name, namespace=namespace
        )
        containers = dep.spec.template.spec.containers

        container_info = [
            {
                "name": c.name,
                "requests": c.resources.requests if c.resources else None,
                "limits": c.resources.limits if c.resources else None,
            }
            for c in containers
        ]

        return {
            "deployment": deployment_name,
            "namespace": namespace,
            "replicas": dep.spec.replicas,
            "containers": container_info,
        }
    except Exception as e:
        return {"error": f"Failed to fetch deployment: {str(e)}"}


@mcp.tool()
def scale_deployment(
    deployment_name: str,
    replicas: int,
    namespace: str = "default",
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Scale a deployment up or down.
    """
    _init_k8s()
    apps_v1 = client.AppsV1Api()

    if dry_run:
        return {
            "status": "dry_run_success",
            "message": f"Would scale deployment '{deployment_name}' to {replicas} replicas.",
        }

    try:
        body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(
            name=deployment_name, namespace=namespace, body=body
        )
        return {
            "status": "applied",
            "message": f"Successfully scaled '{deployment_name}' to {replicas} replicas.",
        }
    except Exception as e:
        return {"error": f"Failed to scale deployment: {str(e)}"}


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
    """
    Update container CPU/Memory limits and requests for a deployment.
    """
    _init_k8s()
    apps_v1 = client.AppsV1Api()

    try:
        dep = apps_v1.read_namespaced_deployment(
            name=deployment_name, namespace=namespace
        )
    except Exception as e:
        return {"error": f"Failed to fetch deployment: {str(e)}"}

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
        dict(current_res.requests) if (current_res and current_res.requests) else {}
    )
    limits = dict(current_res.limits) if (current_res and current_res.limits) else {}

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

    try:
        apps_v1.patch_namespaced_deployment(
            name=deployment_name, namespace=namespace, body=dep
        )
        return {
            "status": "applied",
            "message": f"Successfully updated resources for {deployment_name}/{container_name}",
            "active_requests": requests,
            "active_limits": limits,
        }
    except Exception as e:
        return {"error": f"Failed to apply patch: {str(e)}"}


app = mcp.http_app()
