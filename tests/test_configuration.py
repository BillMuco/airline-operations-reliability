from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_bundle_and_job_configuration_are_valid_yaml():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    job_file = yaml.safe_load((ROOT / "resources" / "airline_job.yml").read_text(encoding="utf-8"))

    assert bundle["bundle"]["name"] == "airline_operations_reliability"
    assert "airline_reliability_pipeline" in job_file["resources"]["jobs"]


def test_ci_and_cd_workflow_is_valid_yaml():
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "ci-cd.yml").read_text(encoding="utf-8")
    )
    assert "test" in workflow["jobs"]
    assert "deploy" in workflow["jobs"]
