import functools
import os
from typing import Any, Dict, Optional
from fastmcp import FastMCP
from kubernetes import client, config
from kubernetes.stream import stream
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

mcp = FastMCP("GKE-Cluster-Inspector")

API_KEY = os.getenv("API_KEY")


# Define ASGI Auth Middleware
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Exclude health check endpoints if necessary
        if request.url.path == "/health":
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or auth_header != f"Bearer {API_KEY}":
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

        return await call_next(request)


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


@mcp.tool()
def check_cross_namespace_connectivity(
    src_pod_name: str,
    src_namespace: str,
    target_pod_name: str,
    target_namespace: str,
    port: int,
    protocol: str = "tcp",
    container_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validates network connectivity between two Pods across different namespaces
    by executing a socket probe inside the source Pod.
    """
    _init_k8s()
    v1 = client.CoreV1Api()

    try:
        # 1. Fetch Target Pod info to get IP and FQDN
        target_pod = v1.read_namespaced_pod(
            name=target_pod_name, namespace=target_namespace
        )
        target_ip = target_pod.status.pod_ip
        target_fqdn = f"{target_pod_name}.{target_namespace}.pod.cluster.local"

        if not target_ip:
            return {
                "status": "FAILED",
                "reason": f"Target pod '{target_pod_name}' in namespace '{target_namespace}' does not have an IP assigned.",
            }

        # 2. Build shell fallback probe commands for the target IP and Port
        # Tries nc, timeout + bash /dev/tcp, or python fallback
        cmd_script = f"""
        if command -v nc >/dev/null 2>&1; then
            nc -z -w 3 {target_ip} {port}
        elif command -v timeout >/dev/null 2>&1; then
            timeout 3 bash -c '</dev/tcp/{target_ip}/{port}'
        else
            python3 -c "import socket; s = socket.socket(); s.settimeout(3); exit(s.connect_ex(('{target_ip}', {port})))"
        fi
        """
        exec_command = ["/bin/sh", "-c", cmd_script]

        # 3. Stream execution on source pod
        exec_kwargs = {
            "name": src_pod_name,
            "namespace": src_namespace,
            "command": exec_command,
            "stderr": True,
            "stdout": True,
            "tty": False,
            "_preload_content": False,
        }
        if container_name:
            exec_kwargs["container"] = container_name

        resp = stream(v1.connect_get_namespaced_pod_exec, **exec_kwargs)
        resp.run_forever(timeout=10)

        stdout = resp.read_stdout()
        stderr = resp.read_stderr()
        return_code = resp.returncode

        is_connected = return_code == 0

        return {
            "connected": is_connected,
            "source": {"pod": src_pod_name, "namespace": src_namespace},
            "target": {
                "pod": target_pod_name,
                "namespace": target_namespace,
                "ip": target_ip,
                "fqdn": target_fqdn,
                "port": port,
            },
            "raw_output": stdout.strip(),
            "error_output": stderr.strip(),
        }

    except Exception as e:
        return {"error": f"Failed to perform connectivity test: {str(e)}"}


@mcp.tool()
def inspect_network_policies(
    source_namespace: str,
    target_namespace: str,
) -> Dict[str, Any]:
    """
    Lists active NetworkPolicies in source and target namespaces to identify isolation rules.
    """
    _init_k8s()
    net_v1 = client.NetworkingV1Api()

    try:
        src_policies = net_v1.list_namespaced_network_policy(namespace=source_namespace)
        tgt_policies = net_v1.list_namespaced_network_policy(namespace=target_namespace)

        return {
            "source_namespace": {
                "namespace": source_namespace,
                "total_policies": len(src_policies.items),
                "policies": [p.metadata.name for p in src_policies.items],
            },
            "target_namespace": {
                "namespace": target_namespace,
                "total_policies": len(tgt_policies.items),
                "policies": [p.metadata.name for p in tgt_policies.items],
            },
        }
    except Exception as e:
        return {"error": f"Failed to list network policies: {str(e)}"}


app = mcp.http_app()
