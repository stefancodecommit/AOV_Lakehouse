# backups/

Backups of `lakehouse/` and `inbox/kvk/`, made by `python -m tools.backup create` and by
Dagster's monthly maintenance job (risk R14 in
[docs/privacy_and_risks.md](../docs/privacy_and_risks.md)). One archive per backup,
`backup_<UTC time>.tar.gz`; the newest three are kept (`backup.keep` in config.yaml).

The archives are not in git (they hold the data). This folder is on the same disk as the
lakehouse, so it protects against mistakes (a wrong delete, a broken table), not against
a broken disk or a lost laptop: point `backup.path` at another disk or a synchronised
folder in `config.local.yaml` for that, with `require_other_disk: true`. Until then each
backup logs a warning that it is on the same disk. How to restore: INSTRUCTIONS.md,
section 7.
