"""Headless run of the Streamlit investigation console against a processed warehouse."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from fraud_platform.lite_pipeline import run  # noqa: E402
from fraud_platform.simulator.generator import FileSink, Simulator  # noqa: E402
from fraud_platform.sinks.warehouse import build_sink  # noqa: E402

APP = str(Path(__file__).resolve().parents[1] / "apps" / "investigation_console" / "app.py")


@pytest.fixture
def console_env(settings, rules, tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    end = datetime.now(timezone.utc).replace(microsecond=0)
    FileSink(settings["source"]["files_path"]).send(
        Simulator(60, seed=5).generate_window(end - timedelta(minutes=240), 240, 4))
    run(settings["source"]["files_path"], str(tmp_path / "ck"), settings, rules)
    monkeypatch.setenv("FRAUD__WAREHOUSE__TYPE", "duckdb")
    monkeypatch.setenv("FRAUD__WAREHOUSE__DUCKDB_PATH", settings["warehouse"]["duckdb_path"])
    return build_sink(settings)


def test_all_pages_render_and_disposition_is_saved(console_env):
    for page in ["Ringkasan", "Antrean Alert", "Kualitas Data & Pipeline"]:
        at = AppTest.from_file(APP, default_timeout=60).run()
        at.sidebar.radio[0].set_value(page).run()
        assert not at.exception, at.exception

    at = AppTest.from_file(APP, default_timeout=60).run()
    at.sidebar.radio[0].set_value("Antrean Alert").run()
    assert len(at.warning) >= 1                       # rule explanations of the selected alert
    [r for r in at.radio if "CONFIRMED_FRAUD" in r.options][0].set_value("CONFIRMED_FRAUD")
    at.text_input[0].input("analis-1")
    [b for b in at.button if b.label == "Simpan disposisi"][0].click().run()
    assert [s.value for s in at.success] == ["Disposisi tersimpan"]
    fb = console_env.query("SELECT disposition, analyst FROM core.fraud_case_feedback")
    assert fb.values.tolist() == [["CONFIRMED_FRAUD", "analis-1"]]
