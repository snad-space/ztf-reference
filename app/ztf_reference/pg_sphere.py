from __future__ import annotations

import math as m
from dataclasses import dataclass

from asyncpg import Connection


@dataclass
class SPoint:
    ra: float
    dec: float

    @property
    def ra_rad(self) -> float:
        return m.radians(self.ra)

    @property
    def dec_rad(self) -> float:
        return m.radians(self.dec)

    def to_sql(self) -> str:
        return f"({self.ra_rad}, {self.dec_rad})"

    @staticmethod
    def from_sql(s: str) -> SPoint:
        s = s.strip("()")
        ra, dec = (m.degrees(float(x)) for x in s.split(","))
        return SPoint(ra=ra, dec=dec)

    def to_dict(self) -> dict:
        return {"ra": self.ra, "dec": self.dec}


@dataclass
class SCircle:
    point: SPoint
    radius_arcsec: float

    @property
    def radius_rad(self) -> float:
        return m.radians(self.radius_arcsec / 3600.0)

    def to_sql(self) -> str:
        return f"<{self.point.to_sql()}, {self.radius_rad}>"

    @staticmethod
    def from_sql(s: str) -> SCircle:
        s = s.strip("<>")
        point, radius = s.rsplit(",", maxsplit=1)
        point = SPoint.from_sql(point)
        radius_arcsec = m.degrees(float(radius)) * 3600.0
        return SCircle(point=point, radius_arcsec=radius_arcsec)


async def connection_setup(con: Connection):
    await con.set_type_codec(
        "spoint",
        encoder=SPoint.to_sql,
        decoder=SPoint.from_sql,
        format="text",
    )
    await con.set_type_codec(
        "scircle",
        encoder=SCircle.to_sql,
        decoder=SCircle.from_sql,
        format="text",
    )
