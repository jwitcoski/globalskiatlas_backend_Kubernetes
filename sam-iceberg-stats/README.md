# Iceberg stats API (Lambda + API Gateway)

Serves the JSON at **`/api/iceberg-stats`** so the Global Ski Atlas Download Data page can show live Iceberg table counts and versioning.

The Lambda reads `s3://IcebergStatsBucket/iceberg-stats/latest.json` (populated by `scripts/upload_iceberg_stats.py`).

## Deploy

1. **Build and deploy** (from this folder; requires [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) and `aws configure`):

   ```bash
   cd sam-iceberg-stats
   sam build
   sam deploy --guided
   ```

   When prompted, set **IcebergStatsBucket** = `globalskiatlas-backend-k8s-output` (or your bucket). Other prompts can use defaults.

   **One-shot deploy** (no prompts) after first `--guided` run, or with a parameter override:

   ```bash
   sam build && sam deploy --parameter-overrides IcebergStatsBucket=globalskiatlas-backend-k8s-output
   ```

2. **Populate the JSON** (once, or on a schedule after `register_iceberg.py`):

   ```powershell
   py -3.11 scripts/upload_iceberg_stats.py --s3-bucket globalskiatlas-backend-k8s-output
   ```

3. **CloudFront** (if your site is behind CloudFront):

   - Add a **behavior** for path pattern `api/iceberg-stats` (or `api/iceberg-stats*`).
   - Origin: the **API Gateway** origin (same as your wiki or other API).
   - Or use the **ApiUrl** output from `sam deploy` as the origin for this behavior.

After that, `https://your-domain/api/iceberg-stats` returns the JSON (or use the API Gateway URL from the stack output).

## Outputs

- **ApiUrl** – API Gateway base URL; append `/api/iceberg-stats` for the stats endpoint.

## Merging into an existing frontend SAM stack

If you already have a SAM/CloudFront stack for globalskiatlas:

- Add the `IcebergStatsBucket` parameter and the `IcebergStatsFunction` + `IcebergStatsApi` resources from `template.yaml` to your template.
- Add a route (e.g. `GET /api/iceberg-stats`) that invokes `IcebergStatsFunction`.
- Add a CloudFront behavior for `api/iceberg-stats` to your existing API Gateway origin.
