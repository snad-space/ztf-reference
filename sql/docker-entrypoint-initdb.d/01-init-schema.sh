#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS pg_sphere;

    CREATE USER app;
    CREATE USER ingest;

    CREATE TABLE quadrant (
        fieldid     integer    NOT NULL,
        filter      varchar(2) NOT NULL CHECK (filter IN ('zg', 'zr', 'zi')),
        ccdid       smallint   NOT NULL,
        qid         smallint   NOT NULL,
        magzp       real       NOT NULL,
        magzp_rms   real       NOT NULL,
        magzp_unc   real       NOT NULL,
        infobits    integer    NOT NULL,
        PRIMARY KEY (fieldid, filter, ccdid, qid)
    );

    CREATE TABLE refpsfcat (
        fieldid     integer          NOT NULL,
        filter      varchar(2)       NOT NULL,
        ccdid       smallint         NOT NULL,
        qid         smallint         NOT NULL,
        sourceid    integer          NOT NULL,
        xpos        real             NOT NULL,
        ypos        real             NOT NULL,
        ra          double precision NOT NULL,
        dec         double precision NOT NULL,
        coord       spoint           NOT NULL,
        flux        real             NOT NULL,
        sigflux     real             NOT NULL,
        mag         real             NOT NULL,
        sigmag      real             NOT NULL,
        snr         real             NOT NULL,
        chi         real             NOT NULL,
        sharp       real             NOT NULL,
        flags       smallint         NOT NULL,
        PRIMARY KEY (fieldid, filter, ccdid, qid, sourceid),
        FOREIGN KEY (fieldid, filter, ccdid, qid) REFERENCES quadrant (fieldid, filter, ccdid, qid)
    );

    CREATE INDEX idx_refpsfcat_coord ON refpsfcat USING GIST (coord);
    CREATE INDEX idx_refpsfcat_quadrant ON refpsfcat (fieldid, filter, ccdid, qid);

    CREATE VIEW refpsfcat_full AS
    SELECT r.fieldid, r.filter, r.ccdid, r.qid, r.sourceid,
           r.xpos, r.ypos, r.ra, r.dec, r.coord,
           r.flux, r.sigflux, r.mag, r.sigmag, r.snr, r.chi, r.sharp, r.flags,
           q.magzp, q.magzp_rms, q.magzp_unc, q.infobits
    FROM refpsfcat r
    JOIN quadrant q USING (fieldid, filter, ccdid, qid);

    CREATE TABLE ingest_metadata (
        fieldid       integer    NOT NULL,
        filter        varchar(2) NOT NULL,
        ccdid         smallint   NOT NULL,
        qid           smallint   NOT NULL,
        etag          text,
        last_modified text,
        content_length bigint,
        ingested_at   timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (fieldid, filter, ccdid, qid)
    );

    GRANT SELECT ON quadrant TO app;
    GRANT SELECT ON refpsfcat TO app;
    GRANT SELECT ON refpsfcat_full TO app;
    GRANT SELECT, INSERT, UPDATE, DELETE ON quadrant TO ingest;
    GRANT SELECT, INSERT, UPDATE, DELETE ON refpsfcat TO ingest;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ingest_metadata TO ingest;
    REVOKE CREATE ON SCHEMA public FROM public;
EOSQL
