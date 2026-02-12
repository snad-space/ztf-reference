"""Discover refpsfcat FITS files on IRSA."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

IRSA_BASE = "https://irsa.ipac.caltech.edu/ibe/data/ztf/products/ref/"

FILTER_MAP = {1: "zg", 2: "zr", 3: "zi"}
FILTER_IDS = {"zg": 1, "zr": 2, "zi": 3}


@dataclass(frozen=True)
class FileRef:
    fieldid: int
    filter: str
    ccdid: int
    qid: int

    @property
    def root(self) -> str:
        return "000" if self.fieldid < 1000 else "001"

    @property
    def path(self) -> str:
        return (
            f"{self.root}/field{self.fieldid:06d}/{self.filter}/"
            f"ccd{self.ccdid:02d}/q{self.qid}/"
            f"ztf_{self.fieldid:06d}_{self.filter}_c{self.ccdid:02d}_q{self.qid}_refpsfcat.fits"
        )

    @property
    def url(self) -> str:
        return f"{IRSA_BASE}{self.path}"


def generate_all_refs(
    fieldids: list[int] | None = None,
    filters: list[str] | None = None,
    ccdids: list[int] | None = None,
    qids: list[int] | None = None,
) -> list[FileRef]:
    """Generate FileRef objects for all valid combinations.

    If fieldids is None, discovers them from IRSA.
    """
    if fieldids is None:
        fieldids = discover_fieldids()

    filter_list = filters or ["zg", "zr", "zi"]
    ccdid_list = ccdids or list(range(1, 17))
    qid_list = qids or list(range(1, 5))
    refs = []
    for fieldid in fieldids:
        for filt in filter_list:
            for ccdid in ccdid_list:
                for qid in qid_list:
                    refs.append(
                        FileRef(fieldid=fieldid, filter=filt, ccdid=ccdid, qid=qid)
                    )
    return refs


def discover_fieldids() -> list[int]:
    """Crawl IRSA directory listing to find all field directories."""
    fieldids = []
    for root in ("000", "001"):
        url = f"{IRSA_BASE}{root}/"
        logger.info("Discovering fields from %s", url)
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
        except httpx.HTTPError:
            logger.warning("Failed to list %s", url)
            continue
        for match in re.finditer(r'href="field(\d{6})/"', resp.text):
            fieldids.append(int(match.group(1)))
    fieldids.sort()
    logger.info("Discovered %d fields", len(fieldids))
    return fieldids
