# AWS Deployment: GitHub → ECR → ECS Fargate → S3

**globalskiatlas_backend_Kubernetes** — Docker/ECS backend (distinct from the Lambda-based setup). This guide walks through running the ski atlas pipeline (Iceland first, then continent-wide) on AWS using Docker, ECS Fargate, and S3. The flow is designed for **monthly batch jobs** that extract parquet files into an S3 bucket.

---

## Quick Start (TL;DR)

1. **AWS one-time**: Create S3 bucket, ECR repo, IAM roles (execution + task), ECS cluster, register task definition.
2. **GitHub**: Add secrets (AWS keys, ECR repo, ECS cluster, subnets, security group, S3 bucket).
3. **Push to main**: GitHub Actions builds the image and pushes to ECR.
4. **Run Iceland**: Either manually in Actions (Run workflow → check "Run ECS task") or via `aws ecs run-task`.
5. **Output**: Parquet files appear in `s3://globalskiatlas-backend-k8s-output/iceland/YYYY-MM/`.

**GitHub repo name:** `globalskiatlas_backend_Kubernetes` (to differentiate from the Lambda backend).

---

## Architecture Overview

```
GitHub (push/main)  →  GitHub Actions  →  Build Docker image  →  Push to ECR
                                                                    ↓
S3 Bucket (parquet output)  ←  ECS Fargate Task  ←  Run task (manual or scheduled)
```

**Services used:**
- **ECR**: Store Docker images (private registry)
- **ECS + Fargate**: Run the pipeline in a serverless container (no EC2 to manage)
- **S3**: Store parquet output (partitioned by region/date)
- **IAM**: Task role for S3 write access; execution role for ECS

---

## Step 1: One-time AWS Setup

### 1.1 Create S3 Bucket

```bash
# Replace with your bucket name (must be globally unique)
export BUCKET_NAME=globalskiatlas-backend-k8s-output
aws s3 mb s3://$BUCKET_NAME --region us-east-1

# Optional: enable versioning for history
aws s3api put-bucket-versioning --bucket $BUCKET_NAME \
  --versioning-configuration Status=Enabled
```

**Folder structure in S3:**
```
s3://globalskiatlas-backend-k8s-output/
  iceland/
    2025-02/
      ski_areas.parquet
      ski_areas_analyzed.csv
      ski_areas_analyzed.parquet
      lifts.parquet
      pistes.parquet
      osm_near_winter_sports.parquet
  europe/          # future: continent-wide
    2025-02/
      ...
```

### 1.2 Create ECR Repository

```bash
aws ecr create-repository \
  --repository-name globalskiatlas-backend-k8s-pipeline \
  --region us-east-1
```

Note the **URI** (e.g. `123456789.dkr.ecr.us-east-1.amazonaws.com/globalskiatlas-backend-k8s-pipeline`).

### 1.3 Create IAM Roles

**Execution role** (ECS needs this to pull images and write logs):

1. IAM → Roles → Create role
2. Trusted entity: **AWS service** → **Elastic Container Service** → **Elastic Container Service Task**
3. Attach policy: `AmazonECSTaskExecutionRolePolicy`
4. Name: `globalskiatlas-backend-k8s-ecs-execution`

**Task role** (your container uses this to write to S3):

1. Create role, trusted entity: **ECS Task**
2. Attach inline policy (replace `YOUR_BUCKET`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET",
        "arn:aws:s3:::YOUR_BUCKET/*"
      ]
    }
  ]
}
```

3. Name: `globalskiatlas-backend-k8s-ecs-task`

### 1.4 Create ECS Cluster (optional but recommended)

```bash
aws ecs create-cluster --cluster-name globalskiatlas-backend-k8s --region us-east-1
```

---

## Step 2: GitHub Secrets

In your repo: **Settings → Secrets and variables → Actions**

Add these secrets:

| Secret        | Description                          |
|---------------|--------------------------------------|
| `AWS_ACCESS_KEY_ID` | IAM user access key (for GitHub Actions) |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |
| `AWS_REGION`  | e.g. `us-east-1`                     |
| `ECR_REPOSITORY` | e.g. `globalskiatlas-backend-k8s-pipeline`   |
| `ECS_CLUSTER` | e.g. `globalskiatlas-backend-k8s`                |
| `ECS_SUBNETS` | Comma-separated subnet IDs (private subnets) |
| `ECS_SECURITY_GROUP` | Security group ID (must allow outbound HTTPS) |
| `S3_BUCKET`   | e.g. `globalskiatlas-backend-k8s-output`         |

**Note:** Create an IAM user with permissions for: `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:PutImage`, `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`, and `ecs:RunTask`, `iam:PassRole` for the task role and execution role.

---

## Step 3: Docker Image for AWS

The project includes `Dockerfile.aws` which:

1. Builds a single image with both extract (osmium) and analyze (geopandas) tooling
2. Runs the full Iceland pipeline in one container
3. Uploads parquet + CSV to S3 at the end

**Key difference from local compose:** One container, ephemeral storage, S3 as final destination.

**Test locally** (without S3 upload):
```bash
docker build -f Dockerfile.aws -t globalskiatlas-backend-k8s-iceland .
docker run --rm globalskiatlas-backend-k8s-iceland
# Parquet written to container's /data; upload skipped if S3_BUCKET unset
```

**Test locally with S3** (requires AWS credentials):
```bash
docker run --rm -e S3_BUCKET=your-bucket \
  -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=yyy -e AWS_REGION=us-east-1 \
  globalskiatlas-backend-k8s-iceland
```

---

## Step 4: ECS Task Definitions (region-based size)

The workflow selects **CPU/memory by region** so small regions don’t overpay and large continents don’t OOM. Three task definitions are used:

| Size   | Family name                                  | CPU  | Memory | Ephemeral | Regions |
|--------|----------------------------------------------|------|--------|-----------|---------|
| Small  | `globalskiatlas-backend-k8s-pipeline-small`  | 1 vCPU | 2 GB  | 21 GB     | iceland |
| Medium | `globalskiatlas-backend-k8s-pipeline-medium` | 2 vCPU | 4 GB  | 21 GB     | south-america, africa, australia-oceania |
| Large  | `globalskiatlas-backend-k8s-pipeline-large`  | 4 vCPU | 16 GB | 100 GB    | north-america, europe, asia |

1. **Create CloudWatch log group** (shared by all three):
   ```bash
   aws logs create-log-group --log-group-name /ecs/globalskiatlas-backend-k8s-pipeline
   ```

2. **Register all three task definitions** (from repo root; replace account ID in the JSONs if needed):
   ```bash
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-small.json --region us-east-1
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-medium.json --region us-east-1
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-large.json --region us-east-1
   ```

**Task def JSONs:** `aws/ecs-task-pipeline-small.json`, `aws/ecs-task-pipeline-medium.json`, `aws/ecs-task-pipeline-large.json`. Each uses container name `pipeline`, so the workflow’s overrides apply to all. No GitHub secret for task definition is required—the workflow picks the family from the selected region.

---

## Step 5: GitHub Actions Workflow

The workflow `.github/workflows/deploy-iceland-aws.yml`:

1. **On push to main** (or manual dispatch):
   - Build Docker image
   - Push to ECR
2. **Optional: Run ECS task** (manual `workflow_dispatch` with `run_task: true`, and choose **region**):
   - Selects task size from region (small/medium/large)
   - Runs the ECS task with the new image; task downloads PBF, runs pipeline, uploads to S3

---

## Step 6: Run Iceland on AWS

### Via GitHub Actions (manual)

1. Push your code to `main`
2. Actions → **Deploy Iceland to AWS** → Run workflow
3. Check "Run ECS task after push"
4. Run workflow

### Via AWS CLI

```bash
aws ecs run-task \
  --cluster globalskiatlas-backend-k8s \
  --task-definition globalskiatlas-backend-k8s-iceland \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx,subnet-yyy],securityGroups=[sg-xxx],assignPublicIp=ENABLED}" \
  --overrides '{
    "containerOverrides": [{
      "name": "iceland",
      "environment": [
        {"name": "S3_BUCKET", "value": "globalskiatlas-backend-k8s-output"},
        {"name": "REGION", "value": "iceland"}
      ]
    }]
  }'
```

### Via EventBridge (monthly schedule)

1. EventBridge → Rules → Create rule
2. Schedule: `cron(0 6 1 * ? *)` (1st of month at 06:00 UTC)
3. Target: ECS Task
4. Cluster, task definition, subnets, security group as above

---

## Scaling to Continent-Wide (Europe, North America)

| Region       | PBF Size | Est. Runtime | Suggested CPU/Memory      |
|--------------|----------|--------------|---------------------------|
| Iceland      | ~60 MB   | ~2 min       | 1 vCPU, 2 GB              |
| New Zealand  | ~373 MB  | ~10 min      | 2 vCPU, 4 GB              |
| North America| ~16 GB   | ~4–7 hours   | 4 vCPU, 16 GB, 100 GB disk|

For large regions:

1. Increase task CPU/memory in the task definition
2. Increase `ephemeralStorage` (up to 200 GB for Fargate)
3. Add a `REGION` env var to select PBF URL (e.g. `north-america`, `europe`)
4. Consider **AWS Batch** if you need longer runs or job queues

---

## Cost Estimate (Iceland, monthly)

| Item        | Cost                        |
|-------------|-----------------------------|
| ECR storage | ~$0.10/month (1 image)      |
| Fargate     | ~$0.05/run (2 min × 1 vCPU) |
| S3          | ~$0.01/month (MB of parquet)|
| **Total**   | **~$0.20/month** for Iceland|

---

## Troubleshooting

- **Task fails to start**: Check execution role has `AmazonECSTaskExecutionRolePolicy` and can pull from ECR.
- **S3 upload fails**: Check task role has `s3:PutObject` on the bucket.
- **Out of memory**: The workflow uses small/medium/large task defs by region. If a region still OOMs, edit the corresponding `aws/ecs-task-pipeline-*.json` (e.g. medium → 8 GB), re-register, and re-run.
- **Out of disk**: Increase `ephemeralStorage` (Fargate max 200 GB).
