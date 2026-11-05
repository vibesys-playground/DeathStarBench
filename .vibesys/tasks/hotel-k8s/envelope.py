"""The application tier's capacity envelope: the candidate's manifest, validated before any deploy.

The task owns the data tier (k8s/manifest.yaml: MongoDB, memcached, consul, jaeger, their
Services, the services' ConfigMaps and the frontend Service). The candidate owns the
application tier, `hotelReservation/deploy/app.yaml`: how the eight services' code is
grouped into Deployments, their replicas and the split of the envelope among them, and the
Services its pods call each other through. Both the check (run.py) and the benchmark
(bench.py) call `load()` before they build or apply anything; a violation raises
EnvelopeError, whose message names the file, the object and the key of every violation.

The rules (OBJECTIVE.md, Resources):
- the sum over Deployments of replicas x the containers' limits is at most APP_CPU CPUs and
  APP_MEMORY bytes; requests equal limits (cpu and memory, nothing else) on every container;
- every Deployment runs at least MIN_REPLICAS replicas;
- every container runs the candidate image (`${HOTEL_IMAGE}`) and sets GOMAXPROCS to its CPU
  limit rounded up: the literal ceil(limit), or the downward API (`resourceFieldRef`
  `limits.cpu`, divisor 1, which Kubernetes rounds up the same way);
- only Deployments, Services (ClusterIP) and ConfigMaps, in the job's namespace, with names
  that the task's objects do not use; pod and container fields from an allowlist (no host
  namespaces, node selection, tolerations, privileges, host paths, volumes beyond configMap
  and emptyDir, init containers);
- the task's frontend Service (`frontend:5000`, the load generator's and the oracle's entry
  point) keeps its selector: at least one Deployment's pods carry its labels, and no
  candidate pod carries the selector of another task Service.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any

import yaml

TASK_DIR = Path(__file__).resolve().parent
TASK_MANIFEST = TASK_DIR / "k8s" / "manifest.yaml"
# The reference application (the base topology): used when a candidate has no app.yaml
# (candidates from before the envelope), and kept identical to the base's own copy.
REFERENCE_APP = TASK_DIR / "k8s" / "app.yaml"
CANDIDATE_APP = Path("hotelReservation") / "deploy" / "app.yaml"

APP_CPU = Fraction(16)  # CPUs, the sum of the application containers' limits
APP_MEMORY = 32 * 2**30  # bytes (32 GiB)
MIN_REPLICAS = 2
MIN_CPU = Fraction(1, 10)  # per container
MIN_MEMORY = 64 * 2**20  # per container (64 MiB)
IMAGE = "${HOTEL_IMAGE}"
VARIABLES = ("NAMESPACE", "HOTEL_IMAGE")
FRONTEND_SERVICE = "frontend"
FRONTEND_PORT = 5000
RESERVED_LABELS = ("tier", "backend")  # the task selects its data tier by them

ALLOWED_KINDS = {("apps/v1", "Deployment"), ("v1", "Service"), ("v1", "ConfigMap")}
METADATA_KEYS = {"name", "namespace", "labels", "annotations"}
DEPLOYMENT_SPEC_KEYS = {
    "replicas", "selector", "template", "strategy", "minReadySeconds",
    "revisionHistoryLimit", "progressDeadlineSeconds",
}
TEMPLATE_METADATA_KEYS = {"labels", "annotations"}
POD_SPEC_KEYS = {
    "containers", "volumes", "hostname", "subdomain", "restartPolicy",
    "terminationGracePeriodSeconds", "dnsPolicy", "enableServiceLinks",
    "automountServiceAccountToken", "serviceAccountName",
    # allowed only empty, as the base writes them:
    "affinity", "tolerations", "nodeSelector",
}
EMPTY_ONLY_POD_KEYS = ("affinity", "tolerations", "nodeSelector")
CONTAINER_KEYS = {
    "name", "image", "imagePullPolicy", "resources", "ports", "env", "command", "args",
    "volumeMounts", "workingDir", "readinessProbe", "livenessProbe", "startupProbe",
    "lifecycle", "terminationMessagePath", "terminationMessagePolicy", "securityContext",
}
CONTAINER_SECURITY_KEYS = {
    "allowPrivilegeEscalation", "readOnlyRootFilesystem", "runAsNonRoot", "runAsUser",
    "runAsGroup", "capabilities",
}
PORT_KEYS = {"containerPort", "name", "protocol"}
ENV_SOURCES = {"fieldRef", "resourceFieldRef", "configMapKeyRef"}
VOLUME_SOURCES = {"configMap", "emptyDir"}
SERVICE_SPEC_KEYS = {"type", "ports", "selector", "clusterIP", "sessionAffinity", "publishNotReadyAddresses"}
SERVICE_PORT_KEYS = {"name", "port", "targetPort", "protocol"}
CONFIGMAP_KEYS = {"apiVersion", "kind", "metadata", "data", "binaryData", "immutable"}

DNS_LABEL = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
_OBJECT_PATH = re.compile(r"^((?:Deployment|Service|ConfigMap) [^ .]+)\.(.*)$")
_VARIABLE = re.compile(r"\$\{([^}]*)\}")
_QUANTITY = re.compile(r"^([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(m|k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$")
_SUFFIX = {
    None: Fraction(1), "m": Fraction(1, 1000),
    "k": Fraction(10**3), "M": Fraction(10**6), "G": Fraction(10**9), "T": Fraction(10**12),
    "P": Fraction(10**15), "E": Fraction(10**18),
    "Ki": Fraction(2**10), "Mi": Fraction(2**20), "Gi": Fraction(2**30), "Ti": Fraction(2**40),
    "Pi": Fraction(2**50), "Ei": Fraction(2**60),
}


class EnvelopeError(ValueError):
    """The candidate's application manifest breaks a rule; the message lists every violation."""

    def __init__(self, source: str, problems: list[str]) -> None:
        self.problems = problems
        super().__init__(
            f"{source} breaks the application tier's rules (OBJECTIVE.md, Resources):\n"
            + "\n".join(f"  - {p}" for p in problems)
        )


@dataclass(frozen=True)
class AppDeployment:
    name: str
    replicas: int
    cpu: Fraction  # per pod, the sum of its containers' limits
    memory: int  # per pod, bytes
    selector: dict[str, str]
    labels: dict[str, str]  # of the pod template


@dataclass(frozen=True)
class App:
    """A validated application tier."""

    source: str
    text: str  # the manifest as written (variables not substituted)
    deployments: tuple[AppDeployment, ...]
    frontend: tuple[str, ...]  # the Deployments whose pods the frontend Service selects

    @property
    def names(self) -> list[str]:
        return [d.name for d in self.deployments]

    @property
    def cpu(self) -> Fraction:
        return sum((d.replicas * d.cpu for d in self.deployments), Fraction(0))

    @property
    def memory(self) -> int:
        return sum(d.replicas * d.memory for d in self.deployments)

    def summary(self) -> str:
        parts = ", ".join(
            f"{d.name} {d.replicas}x({format_cpu(d.cpu)} CPU, {d.memory / 2**30:g} GiB)"
            for d in self.deployments
        )
        return (
            f"{self.source}: {format_cpu(self.cpu)} of {format_cpu(APP_CPU)} CPUs, "
            f"{self.memory / 2**30:g} of {APP_MEMORY / 2**30:g} GiB: {parts}"
        )


def format_cpu(value: Fraction) -> str:
    return f"{float(value):g}"


def parse_quantity(value: Any) -> Fraction:
    """A Kubernetes quantity (`1`, `1500m`, `0.5`, `2Gi`, `512Mi`) as an exact Fraction."""
    if isinstance(value, bool):
        raise ValueError(f"{value!r} is not a quantity")
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, float):
        return Fraction(str(value))
    if not isinstance(value, str):
        raise ValueError(f"{value!r} is not a quantity")
    match = _QUANTITY.match(value.strip())
    if not match:
        raise ValueError(f"{value!r} is not a quantity (want for example 1, 500m, 2Gi)")
    return Fraction(match.group(1)) * _SUFFIX[match.group(2)]


def gomaxprocs(cpu: Fraction) -> int:
    """GOMAXPROCS for a CPU limit: rounded up, as the downward API's limits.cpu (divisor 1) is."""
    return max(1, math.ceil(cpu))


def _labels(value: Any) -> dict[str, str]:
    return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}


def _selects(selector: dict[str, str], labels: dict[str, str]) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def _documents(text: str, source: str) -> list[Any]:
    try:
        return [doc for doc in yaml.safe_load_all(text) if doc is not None]
    except yaml.YAMLError as error:
        raise EnvelopeError(source, [f"not YAML: {error}"]) from None


@dataclass(frozen=True)
class TaskObjects:
    """What the candidate's manifest must not collide with, and the frontend Service."""

    names: frozenset[tuple[str, str]]  # (kind, name)
    service_selectors: dict[str, dict[str, str]]  # task Service -> selector
    pod_labels: tuple[tuple[str, dict[str, str]], ...]  # task Deployment -> template labels
    configmaps: frozenset[str]


def task_objects(text: str) -> TaskObjects:
    documents = _documents(text, "k8s/manifest.yaml")
    names, selectors, pods, configmaps = set(), {}, [], set()
    for doc in documents:
        kind, name = doc["kind"], doc["metadata"]["name"]
        names.add((kind, name))
        if kind == "Service":
            selectors[name] = _labels(doc["spec"].get("selector"))
        elif kind == "Deployment":
            pods.append((name, _labels(doc["spec"]["template"]["metadata"].get("labels"))))
        elif kind == "ConfigMap":
            configmaps.add(name)
    return TaskObjects(frozenset(names), selectors, tuple(pods), frozenset(configmaps))


class _Checker:
    def __init__(self, source: str, task: TaskObjects) -> None:
        self.source = source
        self.task = task
        self.problems: list[str] = []

    def fail(self, where: str, message: str) -> None:
        # "Deployment geo.spec.replicas" reads "Deployment geo: spec.replicas".
        where = _OBJECT_PATH.sub(r"\1: \2", where)
        self.problems.append(f"{where}: {message}")

    def keys(self, where: str, value: Any, allowed: set[str]) -> dict:
        if not isinstance(value, dict):
            self.fail(where, "want a mapping")
            return {}
        for key in sorted(set(value) - allowed):
            self.fail(f"{where}.{key}", "not allowed")
        return value

    # -- objects --------------------------------------------------------------------------------

    def metadata(self, where: str, obj: dict) -> tuple[str, dict]:
        metadata = self.keys(f"{where}.metadata", obj.get("metadata"), METADATA_KEYS)
        name = metadata.get("name")
        if not isinstance(name, str) or not DNS_LABEL.match(name):
            self.fail(f"{where}.metadata.name", f"{name!r} is not a DNS label")
            name = str(name)
        if metadata.get("namespace", "${NAMESPACE}") != "${NAMESPACE}":
            self.fail(f"{where}.metadata.namespace", "must be ${NAMESPACE} or absent")
        labels = _labels(metadata.get("labels"))
        for key in RESERVED_LABELS:
            if key in labels:
                self.fail(f"{where}.metadata.labels.{key}", "reserved for the task's data tier")
        return name, metadata

    def quantity(self, where: str, value: Any) -> Fraction | None:
        try:
            return parse_quantity(value)
        except ValueError as error:
            self.fail(where, str(error))
            return None

    def container(self, where: str, container: Any) -> tuple[Fraction, int]:
        """Check one container; return its (cpu, memory) limits (0 when invalid)."""
        c = self.keys(where, container, CONTAINER_KEYS)
        name = c.get("name")
        if c.get("image") != IMAGE:
            self.fail(f"{where}.image", f"{c.get('image')!r}: only the candidate image {IMAGE} may run")
        for port_index, port in enumerate(c.get("ports") or []):
            self.keys(f"{where}.ports[{port_index}]", port, PORT_KEYS)
        security = c.get("securityContext")
        if security is not None:
            security = self.keys(f"{where}.securityContext", security, CONTAINER_SECURITY_KEYS)
            if security.get("allowPrivilegeEscalation"):
                self.fail(f"{where}.securityContext.allowPrivilegeEscalation", "must not be true")
            capabilities = security.get("capabilities")
            if capabilities is not None:
                capabilities = self.keys(f"{where}.securityContext.capabilities", capabilities, {"drop"})
        cpu, memory = self.resources(f"{where}.resources", c.get("resources"))
        self.env(f"{where}.env", c.get("env"), name, cpu)
        return cpu, memory

    def resources(self, where: str, value: Any) -> tuple[Fraction, int]:
        if value is None:
            self.fail(where, "missing: every container sets requests and limits of cpu and memory")
            return Fraction(0), 0
        resources = self.keys(where, value, {"requests", "limits"})
        parsed: dict[str, dict[str, Fraction]] = {}
        for side in ("requests", "limits"):
            entries = resources.get(side)
            if not isinstance(entries, dict):
                self.fail(f"{where}.{side}", "missing: set cpu and memory, requests equal to limits")
                parsed[side] = {}
                continue
            self.keys(f"{where}.{side}", entries, {"cpu", "memory"})
            parsed[side] = {}
            for resource in ("cpu", "memory"):
                if resource not in entries:
                    self.fail(f"{where}.{side}.{resource}", "missing")
                    continue
                q = self.quantity(f"{where}.{side}.{resource}", entries[resource])
                if q is not None:
                    parsed[side][resource] = q
        limits, requests = parsed["limits"], parsed["requests"]
        for resource in ("cpu", "memory"):
            if resource in limits and resource in requests and limits[resource] != requests[resource]:
                self.fail(
                    f"{where}.requests.{resource}",
                    f"{resources['requests'][resource]!r} != limits {resources['limits'][resource]!r} "
                    "(requests must equal limits)",
                )
        cpu, memory = limits.get("cpu", Fraction(0)), limits.get("memory", Fraction(0))
        if "cpu" in limits:
            if cpu < MIN_CPU:
                self.fail(f"{where}.limits.cpu", f"{float(cpu):g} is below {float(MIN_CPU):g} CPU")
            elif (cpu * 1000).denominator != 1:
                self.fail(f"{where}.limits.cpu", "finer than 1m")
        if "memory" in limits:
            if memory.denominator != 1:
                self.fail(f"{where}.limits.memory", "not a whole number of bytes")
            elif memory < MIN_MEMORY:
                self.fail(f"{where}.limits.memory", f"below {MIN_MEMORY // 2**20} MiB")
        return cpu, int(memory)

    def env(self, where: str, value: Any, container: Any, cpu: Fraction) -> None:
        entries = value or []
        if not isinstance(entries, list):
            self.fail(where, "want a list")
            return
        found = 0
        for index, entry in enumerate(entries):
            at = f"{where}[{index}]"
            entry = self.keys(at, entry, {"name", "value", "valueFrom"})
            source = entry.get("valueFrom")
            if source is not None:
                source = self.keys(f"{at}.valueFrom", source, ENV_SOURCES)
            if entry.get("name") != "GOMAXPROCS":
                continue
            found += 1
            want = gomaxprocs(cpu) if cpu else None
            if source is None:
                if want is not None and str(entry.get("value")) != str(want):
                    self.fail(
                        f"{at}.value",
                        f"GOMAXPROCS={entry.get('value')!r} but the CPU limit is {float(cpu):g}: "
                        f"want {want} (the limit rounded up) or the downward API's limits.cpu",
                    )
                continue
            ref = source.get("resourceFieldRef")
            if not isinstance(ref, dict) or set(source) != {"resourceFieldRef"}:
                self.fail(f"{at}.valueFrom", "GOMAXPROCS comes from a value or resourceFieldRef limits.cpu")
                continue
            if (
                ref.get("resource") != "limits.cpu"
                or str(ref.get("divisor", "1")) != "1"
                or ref.get("containerName", container) != container
            ):
                self.fail(
                    f"{at}.valueFrom.resourceFieldRef",
                    "GOMAXPROCS must be this container's limits.cpu with divisor 1",
                )
        if found != 1:
            self.fail(where, f"sets GOMAXPROCS {found} times: set it once, to the CPU limit")

    def pod(self, where: str, spec: Any, configmaps: set[str]) -> tuple[Fraction, int]:
        pod = self.keys(where, spec, POD_SPEC_KEYS)
        for key in EMPTY_ONLY_POD_KEYS:
            if pod.get(key):
                self.fail(f"{where}.{key}", "must be empty or absent (no node selection)")
        if pod.get("restartPolicy", "Always") != "Always":
            self.fail(f"{where}.restartPolicy", "must be Always")
        if pod.get("dnsPolicy", "ClusterFirst") != "ClusterFirst":
            self.fail(f"{where}.dnsPolicy", "must be ClusterFirst")
        if pod.get("serviceAccountName", "default") != "default":
            self.fail(f"{where}.serviceAccountName", "the application runs as the default account")
        for index, volume in enumerate(pod.get("volumes") or []):
            at = f"{where}.volumes[{index}]"
            volume = self.keys(at, volume, {"name"} | VOLUME_SOURCES)
            sources = set(volume) & VOLUME_SOURCES
            if len(sources) != 1:
                self.fail(at, "want exactly one of configMap or emptyDir")
            if "configMap" in volume:
                ref = volume["configMap"] if isinstance(volume["configMap"], dict) else {}
                if ref.get("name") not in configmaps:
                    self.fail(f"{at}.configMap.name", f"{ref.get('name')!r} is not a ConfigMap of the task or app")
        containers = pod.get("containers")
        if not isinstance(containers, list) or not containers:
            self.fail(f"{where}.containers", "want at least one container")
            return Fraction(0), 0
        names = [c.get("name") for c in containers if isinstance(c, dict)]
        if len(set(names)) != len(names):
            self.fail(f"{where}.containers", "names repeat")
        cpu, memory = Fraction(0), 0
        for index, container in enumerate(containers):
            c, m = self.container(f"{where}.containers[{index}]", container)
            cpu, memory = cpu + c, memory + m
        return cpu, memory

    def deployment(self, where: str, obj: dict, name: str, configmaps: set[str]) -> AppDeployment:
        if ("Deployment", name) in self.task.names:
            self.fail(where, "the task's data tier owns this Deployment name")
        spec = self.keys(f"{where}.spec", obj.get("spec"), DEPLOYMENT_SPEC_KEYS)
        replicas = spec.get("replicas")
        if isinstance(replicas, bool) or not isinstance(replicas, int):
            self.fail(f"{where}.spec.replicas", f"{replicas!r}: set an integer of at least {MIN_REPLICAS}")
            replicas = 0
        elif replicas < MIN_REPLICAS:
            self.fail(
                f"{where}.spec.replicas",
                f"{replicas}: at least {MIN_REPLICAS}, so one pod can restart while another serves",
            )
        selector = self.keys(f"{where}.spec.selector", spec.get("selector"), {"matchLabels"})
        match = _labels(selector.get("matchLabels"))
        if not match:
            self.fail(f"{where}.spec.selector.matchLabels", "want a non-empty matchLabels selector")
        template = spec.get("template") if isinstance(spec.get("template"), dict) else {}
        self.keys(f"{where}.spec.template", template, {"metadata", "spec"})
        metadata = self.keys(f"{where}.spec.template.metadata", template.get("metadata") or {}, TEMPLATE_METADATA_KEYS)
        labels = _labels(metadata.get("labels"))
        if match and not _selects(match, labels):
            self.fail(f"{where}.spec.selector.matchLabels", "does not select the template's labels")
        for service, service_selector in self.task.service_selectors.items():
            if service != FRONTEND_SERVICE and _selects(service_selector, labels):
                self.fail(
                    f"{where}.spec.template.metadata.labels",
                    f"match the task's Service {service}: only the data tier serves it",
                )
        for task_deployment, task_labels in self.task.pod_labels:
            if _selects(match, task_labels):
                self.fail(f"{where}.spec.selector", f"selects the task's {task_deployment} pods")
        cpu, memory = self.pod(f"{where}.spec.template.spec", template.get("spec"), configmaps)
        return AppDeployment(name, replicas, cpu, memory, match, labels)

    def service(self, where: str, obj: dict, name: str) -> None:
        if ("Service", name) in self.task.names:
            self.fail(where, "the task owns this Service (the data tier's, or the frontend's)")
        spec = self.keys(f"{where}.spec", obj.get("spec"), SERVICE_SPEC_KEYS)
        if spec.get("type", "ClusterIP") != "ClusterIP":
            self.fail(f"{where}.spec.type", "must be ClusterIP")
        if spec.get("clusterIP", "None") != "None":
            self.fail(f"{where}.spec.clusterIP", "absent or None")
        for index, port in enumerate(spec.get("ports") or []):
            self.keys(f"{where}.spec.ports[{index}]", port, SERVICE_PORT_KEYS)


def validate(text: str, *, source: str, task_text: str) -> App:
    """Check the candidate's application manifest against the envelope; raise EnvelopeError."""
    task = task_objects(task_text)
    check = _Checker(source, task)
    for match in _VARIABLE.finditer(text):
        if match.group(1) not in VARIABLES:
            line = text.count("\n", 0, match.start()) + 1
            check.fail(f"line {line}", f"unknown variable ${{{match.group(1)}}} (known: NAMESPACE, HOTEL_IMAGE)")
    documents = _documents(text, source)
    objects: list[tuple[str, str, dict]] = []
    seen: set[tuple[str, str]] = set()
    for index, doc in enumerate(documents):
        where = f"document {index + 1}"
        if not isinstance(doc, dict):
            check.fail(where, "not a mapping")
            continue
        kind = (doc.get("apiVersion"), doc.get("kind"))
        if kind not in ALLOWED_KINDS:
            check.fail(where, f"{kind[1]} ({kind[0]}) not allowed: only Deployment (apps/v1), Service and ConfigMap (v1)")
            continue
        where = f"{doc['kind']} {(doc.get('metadata') or {}).get('name')}"
        name, _ = check.metadata(where, doc)
        if (doc["kind"], name) in seen:
            check.fail(where, "defined twice")
        seen.add((doc["kind"], name))
        objects.append((doc["kind"], name, doc))
    configmaps = set(task.configmaps)
    for kind, name, doc in objects:
        if kind == "ConfigMap":
            where = f"ConfigMap {name}"
            if name in task.configmaps:
                check.fail(where, "the task owns this ConfigMap")
            check.keys(where, doc, CONFIGMAP_KEYS)
            configmaps.add(name)
    deployments: list[AppDeployment] = []
    for kind, name, doc in objects:
        where = f"{kind} {name}"
        if kind == "Deployment":
            deployments.append(check.deployment(where, doc, name, configmaps))
        elif kind == "Service":
            check.service(where, doc, name)
    if not deployments:
        check.fail("manifest", "has no Deployment")
    for a in deployments:
        for b in deployments:
            if a is not b and _selects(a.selector, b.labels):
                check.fail(f"Deployment {a.name}.spec.selector", f"also selects the pods of Deployment {b.name}")
    frontend_selector = task.service_selectors.get(FRONTEND_SERVICE, {})
    frontend = tuple(d.name for d in deployments if _selects(frontend_selector, d.labels))
    if not frontend:
        labels = ", ".join(f"{k}: {v}" for k, v in sorted(frontend_selector.items()))
        check.fail(
            f"Service {FRONTEND_SERVICE}",
            f"no Deployment's pods carry its selector ({labels}): the frontend on port "
            f"{FRONTEND_PORT} must be served by the application's pods",
        )
    app = App(source=source, text=text, deployments=tuple(deployments), frontend=frontend)
    if app.cpu > APP_CPU:
        check.fail(
            "envelope",
            f"the CPU limits sum to {format_cpu(app.cpu)} CPUs over the {format_cpu(APP_CPU)} of the "
            "application tier (" + ", ".join(f"{d.name}: {d.replicas} x {format_cpu(d.cpu)}" for d in deployments) + ")",
        )
    if app.memory > APP_MEMORY:
        check.fail(
            "envelope",
            f"the memory limits sum to {app.memory / 2**30:g} GiB over the {APP_MEMORY / 2**30:g} GiB of the "
            "application tier (" + ", ".join(f"{d.name}: {d.replicas} x {d.memory / 2**30:g} GiB" for d in deployments) + ")",
        )
    if check.problems:
        raise EnvelopeError(source, check.problems)
    return app


def load(project_root: Path, *, log=None) -> App:  # noqa: ANN001
    """The candidate's application tier, validated; the reference one when it has none."""
    path = project_root / CANDIDATE_APP
    if path.is_file() and not path.is_symlink():
        text, source = path.read_text(), CANDIDATE_APP.as_posix()
    elif path.exists() or path.is_symlink():
        raise EnvelopeError(CANDIDATE_APP.as_posix(), ["must be a regular file"])
    else:
        text, source = REFERENCE_APP.read_text(), "k8s/app.yaml (the reference: the candidate has no " + CANDIDATE_APP.as_posix() + ")"
    app = validate(text, source=source, task_text=TASK_MANIFEST.read_text())
    if log is not None:
        log(app.summary())
    return app


def compose(app: App) -> str:
    """The whole manifest: the task's data tier and configuration, then the application."""
    return TASK_MANIFEST.read_text().rstrip("\n") + "\n---\n# The application tier: " + app.source + "\n" + app.text
