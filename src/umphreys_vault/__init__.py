"""umphreys-vault: Postgres data vault + ETL for Umphrey's McGee setlists.

Single upstream source: the All Things Umphreys (ATU) public REST API v2.
Unlike the Phish vault this was templated from, there is no audio source and
no reviews method, and per-song gap/times-played/debut are computed by the
ETL aggregate pass rather than pulled (the ATU API exposes none of them).

Public entrypoint is the Click CLI in :mod:`umphreys_vault.cli`
(``umphreys-vault``).
"""

__version__ = "0.1.0"
