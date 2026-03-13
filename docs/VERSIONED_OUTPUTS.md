# Versioned combined output

For **VectorLedger** or audit workflows, you can keep versioned GeoParquet snapshots by copying combined output to a dated path or S3 prefix after each full run.

## Option 1: Dated local path

After `combine_regions.py` (or your world pipeline) writes to `output/combined/`, copy that directory to a versioned path:

```bash
# Example: copy to output/combined/2026-03-10/
cp -r output/combined output/combined/$(date +%Y-%m-%d)
```

Or on Windows (PowerShell):

```powershell
Copy-Item -Recurse output\combined "output\combined\$(Get-Date -Format 'yyyy-MM-dd')"
```

Use the same pattern in CI or a wrapper script so each run produces a snapshot under `output/combined/<YYYY-MM-DD>/`.

## Option 2: S3 prefix

If you upload combined output to S3, use a date (or version) prefix so each run is a distinct snapshot:

```bash
# Example: s3://your-bucket/combined/2026-03-10/
aws s3 sync output/combined s3://YOUR_BUCKET/combined/$(date +%Y-%m-%d)/
```

Then the ledger or downstream tools can point at `s3://bucket/combined/YYYY-MM-DD/*.parquet` as the snapshot for that version.

## Automation

- **CI:** Add a step after the pipeline that copies `output/combined` to `output/combined/<version>` or syncs to S3 with a date prefix.
- **Script:** Optionally add a `--version` or `--date` flag to a wrapper around `combine_regions.py` that writes to `output/combined/<version>/` and optionally uploads to S3. See the main plan for Option B (code change) if you need that.

No code change in this repo is required for Option 1 or 2; use your existing pipeline and add the copy/sync step where you run it.
