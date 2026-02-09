import math
from pathlib import Path

import pytest

from ztf_reference_ingest.discover import FileRef, generate_all_refs
from ztf_reference_ingest.fits import parse_fits


FIXTURES_DIR = Path(__file__).parent / "fixtures"
EXAMPLE_FITS = FIXTURES_DIR / "ztf_000202_zg_c10_q1_refpsfcat.fits"


class TestFileRef:
    def test_root_small_field(self):
        ref = FileRef(fieldid=202, filter="zg", ccdid=10, qid=1)
        assert ref.root == "000"

    def test_root_large_field(self):
        ref = FileRef(fieldid=1500, filter="zr", ccdid=1, qid=1)
        assert ref.root == "001"

    def test_path(self):
        ref = FileRef(fieldid=202, filter="zg", ccdid=10, qid=1)
        assert ref.path == "000/field000202/zg/ccd10/q1/ztf_000202_zg_c10_q1_refpsfcat.fits"

    def test_url(self):
        ref = FileRef(fieldid=202, filter="zg", ccdid=10, qid=1)
        assert "irsa.ipac.caltech.edu" in ref.url


class TestGenerateRefs:
    def test_generates_all_combinations(self):
        refs = generate_all_refs(fieldids=[202], filters=["zg"])
        assert len(refs) == 16 * 4  # 16 ccds * 4 quadrants

    def test_single_field_all_filters(self):
        refs = generate_all_refs(fieldids=[202])
        assert len(refs) == 3 * 16 * 4


class TestParseFits:
    def test_parse_example(self):
        catalog = parse_fits(EXAMPLE_FITS)
        assert catalog.fieldid == 202
        assert catalog.filter == "zg"
        assert catalog.ccdid == 10
        assert catalog.qid == 1
        assert catalog.magzp == pytest.approx(26.325, abs=0.001)
        assert catalog.infobits == 16
        assert len(catalog.rows) > 0

    def test_row_structure(self):
        catalog = parse_fits(EXAMPLE_FITS)
        row = catalog.rows[0]
        # 18 source-level columns
        assert len(row) == 18
        # fieldid, filter, ccdid, qid, sourceid, xpos, ypos, ra, dec, coord, flux, ...
        assert row[0] == 202  # fieldid
        assert row[1] == "zg"  # filter
        assert row[2] == 10  # ccdid
        assert row[3] == 1  # qid
        assert isinstance(row[4], int)  # sourceid
        assert isinstance(row[7], float)  # ra
        assert isinstance(row[8], float)  # dec
        assert isinstance(row[9], str)  # coord (spoint text)

    def test_coord_format(self):
        catalog = parse_fits(EXAMPLE_FITS)
        row = catalog.rows[0]
        coord = row[9]
        # Should be "(ra_rad, dec_rad)" format
        assert coord.startswith("(")
        assert coord.endswith(")")
        parts = coord.strip("()").split(",")
        assert len(parts) == 2
        ra_rad = float(parts[0])
        dec_rad = float(parts[1])
        assert abs(math.degrees(ra_rad) - row[7]) < 0.0001
