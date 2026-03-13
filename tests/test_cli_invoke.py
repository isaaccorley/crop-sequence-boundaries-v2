"""Tests for CLI commands that actually invoke stages (with mocked backends)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from csb.cli import main


def test_create_invokes_run_create(tmp_path: Path):
    runner = CliRunner()
    output_dir = str(tmp_path / "create_out")

    with patch("csb.create.run_create", return_value=Path(output_dir)) as mock:
        result = runner.invoke(main, ["create", "2020", "2022", "-o", output_dir])

    assert result.exit_code == 0
    mock.assert_called_once()
    call_args = mock.call_args
    assert call_args[0][1] == 2020
    assert call_args[0][2] == 2022


def test_prep_invokes_run_prep(tmp_path: Path):
    runner = CliRunner()
    create_dir = tmp_path / "create"
    create_dir.mkdir()
    output_dir = str(tmp_path / "prep_out")

    with patch("csb.prep.run_prep", return_value=Path(output_dir)) as mock:
        result = runner.invoke(
            main, ["prep", "2020", "2022", "--create-dir", str(create_dir), "-o", output_dir]
        )

    assert result.exit_code == 0
    mock.assert_called_once()


def test_distribute_invokes_run_distribute(tmp_path: Path):
    runner = CliRunner()
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()
    output_dir = str(tmp_path / "dist_out")

    with patch("csb.distribute.run_distribute", return_value=Path(output_dir)) as mock:
        result = runner.invoke(
            main, ["distribute", "2020", "2022", "--prep-dir", str(prep_dir), "-o", output_dir]
        )

    assert result.exit_code == 0
    mock.assert_called_once()


def test_run_all_invokes_all_stages(tmp_path: Path):
    runner = CliRunner()
    output_dir = str(tmp_path / "all_out")

    with (
        patch("csb.create.run_create", return_value=Path(tmp_path / "create")) as mock_create,
        patch("csb.prep.run_prep", return_value=Path(tmp_path / "prep")) as mock_prep,
        patch("csb.distribute.run_distribute", return_value=Path(tmp_path / "dist")) as mock_dist,
    ):
        result = runner.invoke(main, ["run-all", "2020", "2022", "-o", output_dir])

    assert result.exit_code == 0, result.output
    mock_create.assert_called_once()
    mock_prep.assert_called_once()
    mock_dist.assert_called_once()
    # No split stage — create is the first stage
    call_args = mock_create.call_args
    assert call_args[0][1] == 2020
    assert call_args[0][2] == 2022


def test_create_uses_default_output(tmp_path: Path):
    """When no -o is given, output path comes from config."""
    runner = CliRunner()

    with patch("csb.create.run_create", return_value=Path("/tmp/out")) as mock:
        result = runner.invoke(main, ["create", "2020", "2022"])

    assert result.exit_code == 0
    mock.assert_called_once()
    # Output dir should be derived from config paths.output
    call_args = mock.call_args
    out_path = call_args[0][3]
    assert "create" in str(out_path)
    assert "2020_2022" in str(out_path)
