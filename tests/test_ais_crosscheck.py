"""Offline regression tests for Transcargo -> AISStream identity matching."""
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Module requires a key at import time but no network connection is made by these tests.
os.environ.setdefault("AISSTREAM_API_KEY", "offline-test-key")
spec = importlib.util.spec_from_file_location("ais", Path(__file__).resolve().parents[1] / "scripts" / "update_aisstream.py")
ais = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ais)


class CrosscheckTests(unittest.TestCase):
    def test_exact_name_match(self):
        manifest = {"msccassandre": {"schedule_name": "MSC CASSANDRE", "mmsi": ""}}
        result = ais.resolve_schedule({"name": "MSC CASSANDRE", "mmsi": "123456789"}, manifest)
        self.assertEqual(result["schedule_name"], "MSC CASSANDRE")

    def test_similar_name_not_accepted(self):
        manifest = {"msccassandre": {"schedule_name": "MSC CASSANDRE", "mmsi": ""}}
        self.assertIsNone(ais.resolve_schedule({"name": "MSC CASSANDRA", "mmsi": "123456789"}, manifest))

    def test_mmsi_matches_when_ais_name_missing(self):
        manifest = {"msccassandre": {"schedule_name": "MSC CASSANDRE", "mmsi": "123456789"}}
        self.assertIsNotNone(ais.resolve_schedule({"name": "Buque sin nombre", "mmsi": "123456789"}, manifest))

    def test_unknown_mmsi_is_not_matched(self):
        manifest = {"msccassandre": {"schedule_name": "MSC CASSANDRE", "mmsi": "123456789"}}
        self.assertIsNone(ais.resolve_schedule({"name": "Buque sin nombre", "mmsi": "987654321"}, manifest))

    def test_registry_requires_nine_digit_mmsi(self):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "identifiers.json"
            p.write_text(json.dumps({"vessels": [
                {"name": "MSC CASSANDRE", "imo": "9785471", "mmsi": None},
                {"name": "ONE AMAZON", "imo": "1234567", "mmsi": "123456789"},
                {"name": "MSC RIKKU", "mmsi": "123"}
            ]}), encoding="utf-8")
            with patch.object(ais, "IDENTIFIERS_DATA", p):
                ids = ais.load_verified_identifiers()
            self.assertNotIn("msccassandre", ids)
            self.assertNotIn("mscrikku", ids)
            self.assertEqual(ids["oneamazon"]["mmsi"], "123456789")

    def test_manifest_filters_cancelled_and_uses_registry(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            schedule = root / "exolgan.json"
            registry = root / "ids.json"
            schedule.write_text(json.dumps({"vessels": [
                {"name": "ONE AMAZON", "terminal": "EXOLGAN", "status": "EN VIAJE", "eta": "2026-11-25T20:00"},
                {"name": "CANCELLED", "terminal": "EXOLGAN", "status": "CANCELADO"},
                {"name": "OTHER", "terminal": "OTHER", "status": "EN VIAJE"}
            ]}), encoding="utf-8")
            registry.write_text(json.dumps({"vessels": [{"name": "ONE AMAZON", "mmsi": "123456789"}]}), encoding="utf-8")
            with patch.object(ais, "EXOLGAN_DATA", schedule), patch.object(ais, "IDENTIFIERS_DATA", registry):
                manifest = ais.load_exolgan_manifest()
            self.assertEqual(list(manifest), ["oneamazon"])
            self.assertEqual(manifest["oneamazon"]["mmsi"], "123456789")


if __name__ == "__main__":
    unittest.main()
