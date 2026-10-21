import copy
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("publication", Path(__file__).with_name("verify-staged-publication.py"))
publication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publication)


class PublicationChecks(unittest.TestCase):
    tag = "v0.8.1"
    source = "a" * 40

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.assets = {}
        sums = []
        for platform, extension in [("linux_amd64", "tar.gz"), ("linux_arm64", "tar.gz"),
                                    ("darwin_amd64", "tar.gz"), ("darwin_arm64", "tar.gz"),
                                    ("windows_amd64", "zip")]:
            name = f"cronos-db_{self.tag}_{platform}.{extension}"
            digest = self.write_asset(name, name.encode())
            sums.append(f"{digest}  {name}\n")
        self.write_asset("SHA256SUMS", "".join(sums).encode())
        archive = f"cronos-db_{self.tag}_linux_amd64_image.tar.gz"
        archive_hash = self.write_asset(archive, b"tested image archive")
        self.config = "sha256:" + "3" * 64
        self.pinned = publication.IMAGE + "@sha256:" + "2" * 64
        self.write_asset("IMAGE_ID", (self.config + "\n").encode())
        self.write_asset("IMAGE_DIGEST", (self.pinned + "\n").encode())
        self.write_asset("IMAGE_ARCHIVE_SHA256", f"{archive_hash}  {archive}\n".encode())
        self.scan = {"Metadata": {"ImageID": self.config}, "Results": []}
        self.write_asset("release-image-scan.json", json.dumps(self.scan).encode())
        proof = {"version": self.tag, "source_revision": self.source, "image_repository": publication.IMAGE,
                 "image_manifest_digest": "sha256:" + "2" * 64, "image_config_digest": self.config,
                 "image_archive_sha256": archive_hash, "staging_run_id": 123}
        self.write_asset("LOCAL_PUBLICATION_EVIDENCE.json", json.dumps(proof).encode())
        self.save_release()

    def write_asset(self, name, data):
        (self.directory / name).write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        self.assets[name] = {"name": name, "digest": "sha256:" + digest}
        return digest

    def save_release(self):
        (self.directory / "release.json").write_text(json.dumps({
            "id": 1, "tag_name": self.tag, "prerelease": False, "draft": True,
            "assets": list(self.assets.values())}))

    def validate(self):
        return publication.validate_files(self.directory, self.tag, self.source)

    def test_complete_staged_release_is_accepted(self):
        _, proof, pinned = self.validate()
        self.assertEqual(pinned, self.pinned)
        self.assertEqual(proof["staging_run_id"], 123)

    def test_wrong_source_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            publication.validate_files(self.directory, self.tag, "b" * 40)

    def test_changed_binary_is_rejected(self):
        name = f"cronos-db_{self.tag}_linux_amd64.tar.gz"
        self.assets[name]["digest"] = "sha256:" + "0" * 64
        self.save_release()
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.validate()

    def test_fixable_high_severity_scan_is_rejected(self):
        self.scan["Results"] = [{"Vulnerabilities": [
            {"Severity": "HIGH", "FixedVersion": "1.2", "VulnerabilityID": "test-advisory"}]}]
        self.write_asset("release-image-scan.json", json.dumps(self.scan).encode())
        self.save_release()
        with self.assertRaisesRegex(ValueError, "Fixable image vulnerability"):
            self.validate()

    @patch.object(publication.subprocess, "check_output")
    def test_failed_staging_is_rejected(self, output):
        output.return_value = json.dumps({"head_sha": self.source, "event": "workflow_dispatch",
                                          "path": ".github/workflows/release.yml",
                                          "status": "completed", "conclusion": "failure"})
        with self.assertRaisesRegex(ValueError, "staging did not pass"):
            publication.verify_staging_run("owner/repo", {"staging_run_id": 123}, self.source)

    @patch.object(publication.subprocess, "run")
    @patch.object(publication.subprocess, "check_output")
    def test_latest_with_different_filesystem_is_rejected(self, output, _run):
        image = {"Os": "linux", "Architecture": "amd64",
                 "Config": {"Labels": {"org.opencontainers.image.revision": self.source,
                                        "org.opencontainers.image.version": self.tag}},
                 "RootFS": {"Type": "layers", "Layers": ["sha256:" + "4" * 64]}}
        latest = copy.deepcopy(image)
        latest["RootFS"]["Layers"] = ["sha256:" + "5" * 64]
        output.side_effect = [json.dumps({"config": {"digest": self.config}})] + [
            json.dumps([item]) for item in [image, image, latest]]
        with self.assertRaisesRegex(ValueError, "does not select the tested image"):
            publication.verify_registry("docker", self.tag, self.source, self.pinned, self.config)

    @patch.object(publication.subprocess, "check_output")
    def test_registry_config_different_from_scan_is_rejected(self, output):
        output.return_value = json.dumps({"config": {"digest": "sha256:" + "0" * 64}})
        with self.assertRaisesRegex(ValueError, "scanned image config"):
            publication.verify_registry("docker", self.tag, self.source, self.pinned, self.config)

    @patch.object(publication.subprocess, "run")
    @patch.object(publication.subprocess, "check_output")
    def test_matching_release_and_latest_are_accepted(self, output, _run):
        image = {"Os": "linux", "Architecture": "amd64",
                 "Config": {"Labels": {"org.opencontainers.image.revision": self.source,
                                        "org.opencontainers.image.version": self.tag}},
                 "RootFS": {"Type": "layers", "Layers": ["sha256:" + "4" * 64]}}
        output.side_effect = [json.dumps({"config": {"digest": self.config}})] + [json.dumps([image])] * 3
        publication.verify_registry("docker", self.tag, self.source, self.pinned, self.config)


if __name__ == "__main__":
    unittest.main()
