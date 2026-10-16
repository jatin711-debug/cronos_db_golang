"""Render deployment variants and assert production startup requirements."""
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "cronos-db"


def check(ephemeral=False, separate_backup=False):
    args = ["helm", "template", "cronos", str(CHART), "--namespace", "cronos-ci",
            "-f", str(CHART / "values-production.yaml"),
            "--set", "headlessService.gossipPort=17946",
            "--set", "nodeSelector.pool=database",
            "--set", "tolerations[0].key=database",
            "--set", "tolerations[0].operator=Exists"]
    if ephemeral:
        args += ["--set", "persistence.enabled=false"]
    if separate_backup:
        args += ["--set", "backupPersistence.enabled=true",
                 "--set", "backupPersistence.storageClass=backup-storage"]
    docs = list(yaml.safe_load_all(subprocess.check_output(args, text=True)))
    sts = next(d for d in docs if d and d["kind"] == "StatefulSet")
    config = next(d["data"] for d in docs if d and d["kind"] == "ConfigMap")
    pod = sts["spec"]["template"]["spec"]
    app = pod["containers"][0]
    env = {e["name"]: e.get("value") for e in app["env"]}
    volumes = {v["name"]: v for v in pod["volumes"]}
    assert config["CRONOS_DEV"] == "false"
    assert config["CRONOS_EXACTLY_ONCE_COMMITS"] == "false"
    assert config["CRONOS_RAFT_DIR"] == "/data/raft"
    assert all(s.endswith(":17946") for s in config["CRONOS_CLUSTER_SEEDS"].split(","))
    assert env["CRONOS_AUTH_POLICY_FILE"] == "/etc/cronos/auth/policy.json"
    for name in ["GOSSIP", "GRPC", "RAFT"]:
        assert "$(POD_NAME)." in env[f"CRONOS_CLUSTER_{name}_ADDR"]
    assert sts["spec"]["podManagementPolicy"] == "Parallel"
    assert "--cluster-bootstrap" in app["args"][0]
    assert "--cluster-seeds" not in app["args"][0]
    assert pod["terminationGracePeriodSeconds"] > 60
    assert app["startupProbe"]["httpGet"]["path"] == "/health"
    # The HTTP port carries the admin API: in production it is served over
    # TLS, and the probes have to speak it.
    assert env["CRONOS_HTTP_TLS_ENABLED"] == "true"
    for probe in ["startupProbe", "livenessProbe", "readinessProbe"]:
        assert app[probe]["httpGet"]["scheme"] == "HTTPS"
    assert pod["nodeSelector"]["pool"] == "database"
    assert pod["tolerations"][0]["key"] == "database"
    assert volumes["encryption"]["emptyDir"]["medium"] == "Memory"
    assert volumes["encryption-source"]["secret"]["defaultMode"] == 0o440
    assert "chmod 600" in pod["initContainers"][0]["args"][0]
    assert ("data" in volumes) == ephemeral
    claims = {claim["metadata"]["name"]: claim for claim in sts["spec"].get("volumeClaimTemplates", [])}
    assert ("data" in claims) != ephemeral
    assert ("backups" in claims) == separate_backup
    if separate_backup:
        assert env["CRONOS_BACKUP_DIR"] == "/backups"
        assert any(v["name"] == "backups" and v["mountPath"] == "/backups" for v in app["volumeMounts"])
        assert claims["backups"]["spec"]["storageClassName"] == "backup-storage"


if __name__ == "__main__":
    subprocess.run(["helm", "lint", str(CHART), "-f", str(CHART / "values-production.yaml")], check=True)
    check()
    check(ephemeral=True)
    check(separate_backup=True)
    check(ephemeral=True, separate_backup=True)
    print("Production chart assertions passed (persistent/ephemeral data and separate backup volumes).")
