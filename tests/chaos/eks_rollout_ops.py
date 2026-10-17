"""Shared pieces of the published-release and rolling-capacity acceptance harnesses (DRN-SCN-019, DRN-SCN-020).

Everything here is either a pure function over observed data (release version rules, OCI image verification,
Prometheus parsing, StatefulSet image patches, audit approval selection) or a thin wrapper that talks to the
strict disposable fixture built by ``eks_fixture.build_fixture``. Mutations go through ``Fixture.mutate`` and
``Fixture.apply``, which refuse to run before ``verify_identity()`` passed. Broker Management API and metrics are
read with ``kubectl exec ... curl`` inside the broker container, so the admin token never leaves the Pod.

Release rules mirror ``internal/domain/release`` (ADR-0018): canonical ``MAJOR.MINOR.PATCH`` without leading zeros,
uint32 parts, same major, minor distance 0 or 1, patch ignored. Image rules mirror the rollout controller contract
§2: ``repository@sha256:<64 hex>`` OCI platform manifest (never an index), ``linux/arm64``, config labels
``org.opencontainers.image.version`` and ``io.narumq.features`` (sorted unique comma-separated tokens).
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

NAMESPACE = "narumq"
STATEFULSET = "narumq"
BROKER_CONTAINER = "narumq"
MANAGEMENT_PORT = 8080
BROKER_REPLICAS = 3
UINT32_MAX = 2 ** 32 - 1
VERSION_LABEL = "org.opencontainers.image.version"
FEATURE_LABEL = "io.narumq.features"
PLATFORM_MANIFEST_TYPES = ("application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json")
INDEX_MEDIA_TYPES = ("application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json")
IMAGE_REFERENCE = re.compile(r"^(?P<repository>[^\s@]+)@(?P<digest>sha256:[0-9a-f]{64})$")
FEATURE_TOKEN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
MAX_FEATURES = 16
WILL_REASONS = ("suppressed_normal_disconnect", "suppressed_reconnected_within_delay", "expected_mqtt311_server_disconnect",
                "expected_zero_or_expired_will_delay", "unexpected_supported_client", "unplanned_failure")
TERMINAL_PHASES = ("Completed", "Failed", "Aborted")
APPROVAL_ACTION = "rollout.pod.delete_approved"
API_PATH = re.compile(r"^/[A-Za-z0-9/_.\-]*(\?[A-Za-z0-9_.=&\-]*)?$")


class ReleaseError(RuntimeError):
    """A release identity or image fails the ADR-0018 / controller-contract rules."""


class MissingPublishedRelease(ReleaseError):
    """The published-release evidence the scenario requires is absent or does not match the images."""


class RegistryToolMissing(RuntimeError):
    """Neither ``crane`` nor ``docker`` (buildx imagetools) is available to inspect the registry."""


class BrokerProbeError(RuntimeError):
    """No broker Pod answered a Management API or metrics read."""


class RolloutTimeout(RuntimeError):
    """A NaruMQRollout did not reach a terminal phase within the deadline."""

    def __init__(self, message: str, samples: list[dict[str, Any]]):
        super().__init__(message)
        self.samples = samples


# Release rules -------------------------------------------------------------------------------------
def parse_version(value: Any) -> tuple[int, int, int]:
    """Canonical ``MAJOR.MINOR.PATCH`` (internal/domain/release.Parse); anything else raises ``ReleaseError``."""
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ReleaseError(f"release version {value!r} is not canonical MAJOR.MINOR.PATCH")
    parts = value.split(".")
    if len(parts) != 3:
        raise ReleaseError(f"release version {value!r} is not canonical MAJOR.MINOR.PATCH")
    numbers = []
    for part in parts:
        if not part.isascii() or not part.isdigit() or (len(part) > 1 and part[0] == "0") or int(part) > UINT32_MAX:
            raise ReleaseError(f"release version {value!r} is not canonical MAJOR.MINOR.PATCH")
        numbers.append(int(part))
    return numbers[0], numbers[1], numbers[2]


def versions_compatible(left: tuple[int, int, int], right: tuple[int, int, int]) -> bool:
    """Same major and minor distance of at most one; patch never matters (release.Compatible)."""
    return left[0] == right[0] and abs(left[1] - right[1]) <= 1


def is_synthetic_version(version: tuple[int, int, int]) -> bool:
    """``0.0.x`` is the repository's synthetic acceptance-fixture series (0.0.0 and 0.0.1 test builds)."""
    return version[0] == 0 and version[1] == 0


def parse_feature_label(value: Any) -> list[str]:
    """``io.narumq.features``: non-empty, comma separated, sorted, unique, valid tokens, at most 16."""
    if not isinstance(value, str) or not value:
        raise ReleaseError("OCI label io.narumq.features is empty or not a string")
    features = value.split(",")
    if len(features) > MAX_FEATURES or any(not FEATURE_TOKEN.match(f) for f in features):
        raise ReleaseError(f"OCI label io.narumq.features {value!r} holds an invalid feature token or too many features")
    if features != sorted(set(features)):
        raise ReleaseError(f"OCI label io.narumq.features {value!r} is not the sorted unique canonical set")
    return features


def split_image_reference(reference: Any) -> tuple[str, str]:
    match = IMAGE_REFERENCE.match(reference) if isinstance(reference, str) else None
    if not match:
        raise ReleaseError(f"image {reference!r} must be <repository>@sha256:<64 lowercase hex>; tags and indexes are rejected")
    return match.group("repository"), match.group("digest")


def repository_of(image: str) -> str:
    """Repository of a digest or tag reference (``host/path@sha256:...`` or ``host/path:tag``)."""
    name = image.split("@", 1)[0]
    last = name.rsplit("/", 1)[-1]
    if ":" in last:
        name = name[: len(name) - len(last)] + last.split(":", 1)[0]
    return name


def normalize_image_config(value: Any) -> dict[str, Any]:
    """Accept the image config as ``crane config`` prints it or as ``buildx imagetools --format {{json .Image}}``."""
    if isinstance(value, dict) and "config" in value and ("os" in value or "architecture" in value):
        return value
    if isinstance(value, dict) and len(value) == 1:
        (inner,) = value.values()
        if isinstance(inner, dict) and "config" in inner:
            return inner
    raise ReleaseError("registry returned no single image config (expected one linux/arm64 platform image)")


def verify_platform_image(reference: str, manifest_bytes: bytes, config: dict[str, Any]) -> dict[str, Any]:
    """Judge one digest-pinned image against the controller contract; returns its release identity.

    The manifest bytes must hash to the digest in the reference (the digest is the registry's immutable identity),
    must be a platform manifest (an OCI index or Docker manifest list is rejected), the image must be linux/arm64,
    and both release labels must be present and canonical.
    """
    repository, digest = split_image_reference(reference)
    actual = "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
    if actual != digest:
        raise ReleaseError(f"manifest bytes hash to {actual}, not the pinned digest {digest}")
    try:
        manifest = json.loads(manifest_bytes)
    except ValueError as error:
        raise ReleaseError(f"manifest of {reference} is not JSON: {error}") from error
    media_type = manifest.get("mediaType")
    if media_type in INDEX_MEDIA_TYPES or "manifests" in manifest:
        raise ReleaseError(f"{reference} is an OCI index/manifest list digest; the controller requires a platform manifest digest")
    if media_type is not None and media_type not in PLATFORM_MANIFEST_TYPES:
        raise ReleaseError(f"{reference} has unsupported manifest media type {media_type!r}")
    if "config" not in manifest or "layers" not in manifest:
        raise ReleaseError(f"{reference} manifest has no config and layers: not an image platform manifest")
    config = normalize_image_config(config)
    if config.get("os") != "linux" or config.get("architecture") != "arm64":
        raise ReleaseError(f"{reference} is {config.get('os')}/{config.get('architecture')}, not linux/arm64")
    labels = config.get("config", {}).get("Labels") or {}
    for label in (VERSION_LABEL, FEATURE_LABEL):
        if label not in labels:
            raise ReleaseError(f"{reference} has no OCI config label {label}")
    version = labels[VERSION_LABEL]
    parse_version(version)
    return {"reference": reference, "repository": repository, "digest": digest, "version": version,
            "features": parse_feature_label(labels[FEATURE_LABEL]), "configDigest": manifest["config"].get("digest"),
            "mediaType": media_type, "os": config["os"], "architecture": config["architecture"]}


def tag_resolves_to(tag_manifest_bytes: bytes, platform_digest: str) -> bool:
    """A published tag names the platform digest directly, or through an index entry for linux/arm64."""
    try:
        manifest = json.loads(tag_manifest_bytes)
    except ValueError:
        return False
    if manifest.get("mediaType") in INDEX_MEDIA_TYPES or "manifests" in manifest:
        return any(entry.get("digest") == platform_digest and entry.get("platform", {}).get("os") == "linux"
                   and entry.get("platform", {}).get("architecture") == "arm64" for entry in manifest.get("manifests", []))
    return "sha256:" + hashlib.sha256(tag_manifest_bytes).hexdigest() == platform_digest


class RegistryClient:
    """Read-only registry access through the tool the development guide uses (``crane``), or Docker buildx."""

    def __init__(self, tool: str = "auto", run: Callable[..., subprocess.CompletedProcess] = subprocess.run):
        if tool == "auto":
            tool = "crane" if shutil.which("crane") else ("docker" if shutil.which("docker") else "")
        if tool not in ("crane", "docker"):
            raise RegistryToolMissing("neither crane nor docker is on PATH (use --registry-tool crane|docker)")
        self.tool, self.run = tool, run

    def _call(self, args: list[str]) -> bytes:
        result = self.run(args, capture_output=True, timeout=180)
        if result.returncode != 0:
            raise ReleaseError(f"{' '.join(args[:4])} failed: {result.stderr.decode('utf-8', 'replace').strip()}")
        return result.stdout

    def manifest(self, reference: str) -> bytes:
        if self.tool == "crane":
            return self._call(["crane", "manifest", reference])
        return self._call(["docker", "buildx", "imagetools", "inspect", "--raw", reference])

    def config(self, reference: str) -> dict[str, Any]:
        if self.tool == "crane":
            return normalize_image_config(json.loads(self._call(["crane", "config", reference])))
        return normalize_image_config(json.loads(self._call(
            ["docker", "buildx", "imagetools", "inspect", reference, "--format", "{{json .Image}}"])))

    def verify(self, reference: str) -> dict[str, Any]:
        return verify_platform_image(reference, self.manifest(reference), self.config(reference))


# Broker reads through kubectl exec ----------------------------------------------------------------
def broker_api(fixture: Any, pod: str, path: str, timeout: float = 30) -> tuple[int, Any]:
    """GET a Management API path inside the broker container; returns (HTTP status, decoded JSON or raw text)."""
    if not API_PATH.match(path):
        raise ValueError(f"unsafe management path {path!r}")
    script = ("curl --silent --max-time 10 -w '\\n%{http_code}' -H \"Authorization: Bearer $(cat /secrets/admin.token)\" "
              f"http://127.0.0.1:{MANAGEMENT_PORT}{path}")
    try:
        output = fixture.kubectl("exec", pod, "-c", BROKER_CONTAINER, "--", "sh", "-c", script, timeout=timeout)
    except Exception as error:  # kubectl exec to a Pod that is stopping fails; the caller tries another Pod
        raise BrokerProbeError(f"GET {path} on {pod} failed: {error}") from error
    body, _, status = output.rpartition("\n")
    try:
        return int(status), json.loads(body) if body.strip() else None
    except ValueError:
        return int(status) if status.strip().isdigit() else 0, body


def ready_broker_pods(fixture: Any) -> list[str]:
    return sorted(name for name, pod in fixture.broker_pods().items() if pod["ready"] and not pod["deleting"])


def _first_answer(fixture: Any, path: str, exclude: tuple[str, ...]) -> tuple[str, Any]:
    tried = []
    for pod in ready_broker_pods(fixture):
        if pod in exclude:
            continue
        try:
            status, body = broker_api(fixture, pod, path)
        except BrokerProbeError as error:
            tried.append(str(error))
            continue
        if status == 200 and isinstance(body, dict):
            return pod, body.get("data", body)
        tried.append(f"{pod} answered {status}")
    raise BrokerProbeError(f"no ready broker Pod answered GET {path}: {tried or 'no ready Pod'}")


def cluster_probe(fixture: Any, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
    """One /api/v1/cluster read from any Ready broker; quorum is judged from the broker's own report."""
    pod, data = _first_answer(fixture, "/api/v1/cluster", exclude)
    return {"at": time.time(), "pod": pod, "quorumAvailable": data.get("quorumAvailable"),
            "leaderNodeId": data.get("leaderNodeId"), "nodesByState": data.get("nodesByState")}


def features_probe(fixture: Any, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
    """Committed feature profile: active features and each voter's release and features."""
    pod, data = _first_answer(fixture, "/api/v1/cluster/features", exclude)
    return {"at": time.time(), "pod": pod, "activeFeatures": list(data.get("activeFeatures") or []),
            "members": [{"nodeId": str(m.get("nodeId")), "releaseVersion": m.get("releaseVersion"),
                         "features": list(m.get("features") or [])} for m in data.get("members", [])]}


def parse_will_outcomes(text: str) -> dict[str, int]:
    """The six fixed ``narumq_will_outcomes_total{reason=...}`` counters of one node; a missing series raises."""
    counters: dict[str, int] = {}
    for reason in WILL_REASONS:
        prefix = f'narumq_will_outcomes_total{{reason="{reason}"}} '
        for line in text.splitlines():
            if line.startswith(prefix):
                value = line[len(prefix):].strip()
                if not value.isdigit():
                    raise BrokerProbeError(f"Will outcome counter {reason} is not a non-negative integer: {line!r}")
                counters[reason] = int(value)
                break
        else:
            raise BrokerProbeError(f"node metrics lack the fixed series narumq_will_outcomes_total{{reason=\"{reason}\"}}")
    return counters


def scrape_will_outcomes(fixture: Any, pod: str) -> dict[str, Any]:
    """Read one broker Pod's Will counters with the Pod UID that identifies the process generation."""
    pods = fixture.broker_pods()
    if pod not in pods:
        raise BrokerProbeError(f"broker Pod {pod} does not exist")
    uid = pods[pod]["uid"]
    script = f"curl --silent --max-time 10 -H \"Authorization: Bearer $(cat /secrets/admin.token)\" http://127.0.0.1:{MANAGEMENT_PORT}/metrics"
    try:
        text = fixture.kubectl("exec", pod, "-c", BROKER_CONTAINER, "--", "sh", "-c", script, timeout=30)
    except Exception as error:
        raise BrokerProbeError(f"metrics scrape of {pod} failed: {error}") from error
    return {"at": time.time(), "pod": pod, "uid": uid, "counters": parse_will_outcomes(text)}


class WillAccumulator:
    """Reset-aware sum of per-process counters.

    ``narumq_will_outcomes_total`` restarts at zero with every broker process, so the delta of one Pod across a
    rolling update would lose the old process's count. Samples are keyed by Pod name and UID: the first sample of a
    new UID adds its whole value (the process started at zero), later samples add the growth since the previous
    sample of that UID. Counts that happen between the last sample of a Pod and its termination are unobservable;
    callers sample right when a drain reports SAFE_TO_STOP to keep that window small.
    """

    def __init__(self) -> None:
        self.totals = {reason: 0 for reason in WILL_REASONS}
        self.last: dict[tuple[str, str], dict[str, int]] = {}
        self.baseline: dict[tuple[str, str], dict[str, int]] = {}

    def start(self, sample: dict[str, Any]) -> None:
        """Baseline: counters of a process that is already running do not belong to this run."""
        key = (sample["pod"], sample["uid"])
        self.baseline[key] = dict(sample["counters"])
        self.last[key] = dict(sample["counters"])

    def add(self, sample: dict[str, Any]) -> None:
        key = (sample["pod"], sample["uid"])
        counters = sample["counters"]
        previous = self.last.get(key, {reason: 0 for reason in WILL_REASONS})
        for reason in WILL_REASONS:
            if counters[reason] < previous[reason]:
                raise BrokerProbeError(f"{sample['pod']} counter {reason} decreased inside one process generation")
            self.totals[reason] += counters[reason] - previous[reason]
        self.last[key] = dict(counters)

    def to_json(self) -> dict[str, Any]:
        """Serializable state so a resumed run keeps counting from the last scrape of every process generation."""
        pack = lambda table: [[pod, uid, counters] for (pod, uid), counters in sorted(table.items())]  # noqa: E731
        return {"totals": dict(self.totals), "last": pack(self.last), "baseline": pack(self.baseline)}

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "WillAccumulator":
        accumulator = cls()
        accumulator.totals = {reason: int(value["totals"][reason]) for reason in WILL_REASONS}
        accumulator.last = {(pod, uid): dict(counters) for pod, uid, counters in value["last"]}
        accumulator.baseline = {(pod, uid): dict(counters) for pod, uid, counters in value["baseline"]}
        return accumulator


# StatefulSet and rollout --------------------------------------------------------------------------
def image_patch_operations(statefulset: dict[str, Any], repository: str, image: str) -> list[dict[str, Any]]:
    """JSON patch replacing the image of every broker-image container of the pod template.

    Only containers whose current repository equals ``repository`` change; the capacity overlay's JWKS and
    security-config containers use another image and stay as they are. The controller never edits the template, so
    the operator-side template change is part of the rollout (Argo CD does it in the GitOps path).
    """
    spec = statefulset["spec"]["template"]["spec"]
    operations = []
    for section in ("initContainers", "containers"):
        for index, container in enumerate(spec.get(section, [])):
            if repository_of(container.get("image", "")) == repository and container["image"] != image:
                operations.append({"op": "replace", "path": f"/spec/template/spec/{section}/{index}/image", "value": image})
    return operations


def set_template_image(fixture: Any, image: str) -> list[dict[str, Any]]:
    """Point the broker StatefulSet template at ``image``; raises when no broker container needs the change."""
    statefulset = fixture.kjson("get", "statefulset", STATEFULSET)
    broker = next(c for c in statefulset["spec"]["template"]["spec"]["containers"] if c["name"] == BROKER_CONTAINER)
    operations = image_patch_operations(statefulset, repository_of(broker["image"]), image)
    if not operations:
        raise RuntimeError(f"StatefulSet template already uses {image}; a rollout would replace nothing")
    fixture.mutate("patch", "statefulset", STATEFULSET, "--type=json", "-p", json.dumps(operations))
    return operations


def rollout_session(fixture: Any, name: str, target: dict[str, Any], registry_secret: str, management_secret: str,
                    helper_image: str = "") -> Any:
    """A rollout-faults ``Session`` used only for its manifest conventions and audit helpers (no proxy, no webhook)."""
    from eks_fixture import rollout_faults
    module = rollout_faults()
    return module.Session(fixture=fixture, proxy=None, management=None, webhook=None, rollout_name=name, target=target,
                          registry_secret=registry_secret, management_secret=management_secret, helper_image=helper_image)


def rollout_target(image: dict[str, Any]) -> dict[str, Any]:
    """``spec.target`` of a NaruMQRollout for a verified release image."""
    return {"image": image["reference"], "releaseVersion": image["version"], "features": list(image["features"])}


def sample_of(status: dict[str, Any]) -> dict[str, Any]:
    return {"at": time.time(), "phase": status.get("phase"), "failureCode": status.get("failureCode"),
            "currentOrdinal": status.get("currentOrdinal", 0), "completed": list(status.get("completedOrdinals", [])),
            "operation": status.get("drainOperationID"), "podRV": status.get("currentPodResourceVersion")}


def drive_rollout(fixture: Any, session: Any, retry_token: str, timeout: float, interval: float = 3.0,
                  observe: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any] | None] | None = None) -> dict[str, Any]:
    """Create the NaruMQRollout, sample it until it is terminal and return status, UID and every sample.

    ``observe(status, sample)`` may return extra fields (quorum, Will counters) that are stored in the sample.
    """
    fixture.apply(session.rollout_manifest(retry_token))
    uid = fixture.kjson("get", "narumqrollout", session.rollout_name)["metadata"]["uid"]
    samples: list[dict[str, Any]] = []
    started, deadline = time.time(), time.monotonic() + timeout
    while True:
        status = fixture.status(session.rollout_name)
        sample = sample_of(status)
        if observe is not None:
            sample.update(observe(status, sample) or {})
        samples.append(sample)
        if status.get("phase") in TERMINAL_PHASES:
            return {"uid": uid, "name": session.rollout_name, "status": status, "samples": samples,
                    "startedAt": started, "finishedAt": time.time()}
        if time.monotonic() >= deadline:
            raise RolloutTimeout(f"rollout {session.rollout_name} was in phase {status.get('phase')} after {timeout:.0f} s", samples)
        time.sleep(interval)


def delete_rollout(fixture: Any, name: str) -> None:
    """Remove a finished or failed NaruMQRollout (a deletion in progress aborts safely, contract §7)."""
    fixture.mutate("delete", "narumqrollout", name, "--ignore-not-found", "--wait=true", "--timeout=300s", timeout=330, check=False)


def approvals_of(events: list[dict[str, Any]], rollout_uid: str) -> list[dict[str, Any]]:
    """Delete-approval audit events of one rollout in append order (the chain order is the decision order)."""
    return [e for e in events if e.get("action") == APPROVAL_ACTION and (e.get("details") or {}).get("rolloutUid") == rollout_uid]


def pod_summary(pods: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: {key: pod[key] for key in ("uid", "image", "imageID", "ready", "deleting")} for name, pod in sorted(pods.items())}


def digest_of_image_id(image_id: str | None) -> str | None:
    if not image_id:
        return None
    index = image_id.rfind("@sha256:")
    return image_id[index + 1:] if index >= 0 else (image_id.split("://", 1)[1] if image_id.startswith("containerd://") else image_id)


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
