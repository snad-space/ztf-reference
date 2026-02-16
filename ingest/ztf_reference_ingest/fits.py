"""Parse ZTF reference PSF catalog FITS files."""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path

import numpy as np
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


def parse_fits(source: Path | bytes) -> ParsedCatalog:
    """Parse a refpsfcat FITS file into structured data.

    Accepts a file path or raw bytes. Returns a ParsedCatalog with header
    metadata and a list of row tuples ready for database insertion.
    """
    fileobj = io.BytesIO(source) if isinstance(source, bytes) else source
    with fits.open(fileobj) as hdul:
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

        # Vectorized column extraction — avoids per-row Python loop
        ra = data["ra"].astype(np.float64)
        dec = data["dec"].astype(np.float64)
        ra_rad = np.radians(ra)
        dec_rad = np.radians(dec)

        sourceids = data["sourceid"].astype(np.int64)
        xpos = data["xpos"].astype(np.float64)
        ypos = data["ypos"].astype(np.float64)
        flux = data["flux"].astype(np.float64)
        sigflux = data["sigflux"].astype(np.float64)
        mag = data["mag"].astype(np.float64)
        sigmag = data["sigmag"].astype(np.float64)
        snr = data["snr"].astype(np.float64)
        chi = data["chi"].astype(np.float64)
        sharp = data["sharp"].astype(np.float64)
        flags = data["flags"].astype(np.int64)

        n = len(data)
        rows = [
            (
                fieldid,
                filt,
                ccdid,
                qid,
                int(sourceids[i]),
                float(xpos[i]),
                float(ypos[i]),
                float(ra[i]),
                float(dec[i]),
                f"({ra_rad[i]}, {dec_rad[i]})",
                float(flux[i]),
                float(sigflux[i]),
                float(mag[i]),
                float(sigmag[i]),
                float(snr[i]),
                float(chi[i]),
                float(sharp[i]),
                int(flags[i]),
            )
            for i in range(n)
        ]

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
