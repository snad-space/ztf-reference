"""Parse ZTF reference PSF catalog FITS files."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

from astropy.io import fits

FILTER_MAP = {1: "zg", 2: "zr", 3: "zi"}


@dataclass
class ParsedCatalog:
    fieldid: int
    filter: str
    ccdid: int
    qid: int
    magzp: float
    magzp_rms: float
    magzp_unc: float
    infobits: int
    rows: list[tuple]


def parse_fits(filepath: Path) -> ParsedCatalog:
    """Parse a refpsfcat FITS file into structured data.

    Returns a ParsedCatalog with header metadata and a list of row tuples
    ready for database insertion (source-level columns only).
    """
    with fits.open(filepath) as hdul:
        header = hdul[0].header
        data = hdul[1].data

        fieldid = int(header["FIELDID"])
        ccdid = int(header["CCDID"])
        qid = int(header["QID"])
        filterid = int(header["FILTERID"])
        filt = FILTER_MAP[filterid]

        magzp = float(header["MAGZP"])
        magzp_rms = float(header["MAGZPRMS"])
        magzp_unc = float(header.get("MAGZPUNC", 0.0))
        infobits = int(header["INFOBITS"])

        rows = []
        for i in range(len(data)):
            row = data[i]
            ra = float(row["ra"])
            dec = float(row["dec"])
            coord_sql = f"({math.radians(ra)}, {math.radians(dec)})"

            rows.append(
                (
                    fieldid,
                    filt,
                    ccdid,
                    qid,
                    int(row["sourceid"]),
                    float(row["xpos"]),
                    float(row["ypos"]),
                    ra,
                    dec,
                    coord_sql,
                    float(row["flux"]),
                    float(row["sigflux"]),
                    float(row["mag"]),
                    float(row["sigmag"]),
                    float(row["snr"]),
                    float(row["chi"]),
                    float(row["sharp"]),
                    int(row["flags"]),
                )
            )

    return ParsedCatalog(
        fieldid=fieldid,
        filter=filt,
        ccdid=ccdid,
        qid=qid,
        magzp=magzp,
        magzp_rms=magzp_rms,
        magzp_unc=magzp_unc,
        infobits=infobits,
        rows=rows,
    )
