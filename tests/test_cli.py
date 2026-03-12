"""Tests for csb.cli."""

from __future__ import annotations

from click.testing import CliRunner

from csb.cli import main


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Crop Sequence Boundaries" in result.output


def test_create_help():
    runner = CliRunner()
    result = runner.invoke(main, ["create", "--help"])
    assert result.exit_code == 0
    assert "start_year" in result.output.lower()


def test_prep_help():
    runner = CliRunner()
    result = runner.invoke(main, ["prep", "--help"])
    assert result.exit_code == 0
    assert "--create-dir" in result.output


def test_distribute_help():
    runner = CliRunner()
    result = runner.invoke(main, ["distribute", "--help"])
    assert result.exit_code == 0
    assert "--prep-dir" in result.output


def test_run_all_help():
    runner = CliRunner()
    result = runner.invoke(main, ["run-all", "--help"])
    assert result.exit_code == 0
    assert "full pipeline" in result.output.lower()
