"""Verify a staged release and its locally published Docker tags before finalizing."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

IMAGE = "docker.io/johnny711dock/cronos-db"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_files(directory, tag, source):
    require(re.fullmatch(r"v\d+\.\d+\.\d+", tag), "Expected a stable release tag")
    release = json.loads((directory / "release.json").read_text())
    proof = json.loads((directory / "LOCAL_PUBLICATION_EVIDENCE.json").read_text())
    require(release["tag_name"] == tag and not release["prerelease"], "Release tag/type mismatch")
    require(proof["version"] == tag and proof["source_revision"] == source, "Publication source mismatch")
    require(proof["image_repository"] == IMAGE, "Unexpected image repository")
    assets = {asset["name"]: asset for asset in release["assets"]}

    def check_asset(name, digest):
        require(re.fullmatch(r"[0-9a-f]{64}", digest), f"Invalid checksum for {name}")
        require(name in assets and assets[name].get("digest") == f"sha256:{digest}",
                f"Uploaded asset checksum mismatch: {name}")

    expected = {f"cronos-db_{tag}_{platform}.{extension}" for platform, extension in [
        ("linux_amd64", "tar.gz"), ("linux_arm64", "tar.gz"),
        ("darwin_amd64", "tar.gz"), ("darwin_arm64", "tar.gz"), ("windows_amd64", "zip")]}
    sums = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        sums[name.lstrip("*")] = digest
    require(set(sums) == expected, "Binary checksums do not cover the five release archives")
    for name, digest in sums.items():
        check_asset(name, digest)
    for name in ["SHA256SUMS", "IMAGE_ID", "IMAGE_DIGEST", "IMAGE_ARCHIVE_SHA256",
                 "release-image-scan.json", "LOCAL_PUBLICATION_EVIDENCE.json"]:
        check_asset(name, hashlib.sha256((directory / name).read_bytes()).hexdigest())

    archive_hash, archive_name = (directory / "IMAGE_ARCHIVE_SHA256").read_text().split()
    require(archive_name.lstrip("*") == f"cronos-db_{tag}_linux_amd64_image.tar.gz", "Image archive name mismatch")
    require(archive_hash == proof["image_archive_sha256"], "Image archive evidence mismatch")
    check_asset(archive_name.lstrip("*"), archive_hash)
    config = (directory / "IMAGE_ID").read_text().strip()
    require(config == proof["image_config_digest"], "Staged image config mismatch")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", config), "Invalid image config digest")
    pinned = (directory / "IMAGE_DIGEST").read_text().strip()
    require(pinned == f'{IMAGE}@{proof["image_manifest_digest"]}', "Pinned image evidence mismatch")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", proof["image_manifest_digest"]), "Invalid image manifest digest")
    scan = json.loads((directory / "release-image-scan.json").read_text())
    require(scan["Metadata"]["ImageID"] == config, "Scan did not examine the staged image")
    for result in scan.get("Results", []):
        for vulnerability in result.get("Vulnerabilities") or []:
            require(not (vulnerability["Severity"] in {"HIGH", "CRITICAL"} and vulnerability.get("FixedVersion")),
                    f'Fixable image vulnerability: {vulnerability["VulnerabilityID"]}')
    return release, proof, pinned


def verify_staging_run(repo, proof, source):
    run_id = int(proof["staging_run_id"])
    require(run_id > 0, "Invalid staging run")
    run = json.loads(subprocess.check_output(["gh", "api", f"repos/{repo}/actions/runs/{run_id}"], text=True))
    require(run["head_sha"] == source and run["event"] == "workflow_dispatch", "Staging source/event mismatch")
    require(run["path"].split("@", 1)[0] == ".github/workflows/release.yml", "Unexpected staging workflow")
    require(run["status"] == "completed" and run["conclusion"] == "success", "Release staging did not pass")


def verify_registry(docker, tag, source, pinned, config_digest):
    manifest = json.loads(subprocess.check_output([docker, "manifest", "inspect", pinned], text=True))
    require(manifest.get("config", {}).get("digest") == config_digest,
            "Registry manifest does not contain the scanned image config")
    reference = None
    for image in [pinned, f"{IMAGE}:{tag[1:]}", f"{IMAGE}:latest"]:
        subprocess.run([docker, "pull", image], check=True)
        info = json.loads(subprocess.check_output([docker, "image", "inspect", image], text=True))[0]
        labels = info["Config"].get("Labels") or {}
        require(info["Os"] == "linux" and info["Architecture"] == "amd64", "Unexpected container platform")
        require(labels.get("org.opencontainers.image.revision") == source, f"Wrong source in {image}")
        require(labels.get("org.opencontainers.image.version") == tag, f"Wrong version in {image}")
        # Engine versions may display different image IDs. Compare the runtime
        # configuration and uncompressed filesystem layer identities directly.
        contents = (info["Config"], info["RootFS"])
        if reference is None:
            reference = contents
        require(contents == reference, f"Registry tag does not select the tested image: {image}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--docker", default="docker")
    args = parser.parse_args()
    _, proof, pinned = validate_files(args.directory, args.tag, args.source)
    verify_staging_run(args.repo, proof, args.source)
    verify_registry(args.docker, args.tag, args.source, pinned, proof["image_config_digest"])
    print("Staging, uploaded checksums, and Docker release/latest identities verified.")


if __name__ == "__main__":
    main()
