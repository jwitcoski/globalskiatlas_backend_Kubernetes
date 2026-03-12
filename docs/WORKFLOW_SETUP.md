# Local PC → GitHub → GitHub Actions → AWS workflow

This doc describes how to **establish** the backend CI/CD flow: develop locally, push to GitHub, have Actions build and push the Docker image to ECR, and optionally run the pipeline on ECS Fargate with output to S3.

---

## Flow overview

```
Local PC                    GitHub                      GitHub Actions              AWS
   |                          |                                |                       |
   |  git push origin main    |                                |                       |
   |------------------------->|  trigger workflow              |                       |
   |                          |------------------------------->|                       |
   |                          |                                |  build Docker image   |
   |                          |                                |  push to ECR          |
   |                          |                                |---------------------->| ECR
   |                          |                                |                       |
   |                          |  (optional) Run ECS task        |  ecs run-task         |
   |                          |  (manual dispatch)             |---------------------->| ECS Fargate
   |                          |                                |                       | → S3 output
```

- **Every push to `main`**: workflow builds the image and pushes to Amazon ECR. No ECS task runs unless you trigger it manually.
- **Manual run**: Actions → Deploy pipeline to AWS → Run workflow → check "Run ECS task after push", choose region → run. That starts an ECS task (Fargate) which runs the pipeline and uploads Parquet to S3.

---

## 1. Local PC

- **Clone** the repo (or you already have it).
- **Develop**: edit scripts, Dockerfile, config under `config/regions.yaml`, etc.
- **Test locally** (optional):  
  `docker build -f Dockerfile.aws -t globalskiatlas-pipeline .`  
  `docker run --rm -e REGION=iceland globalskiatlas-pipeline`  
  (S3 upload is skipped if `S3_BUCKET` is not set.)
- **Commit and push to `main`**:
  ```bash
  git add .
  git commit -m "Your message"
  git push origin main
  ```
- Pushing to `main` triggers the **Deploy pipeline to AWS** workflow (build + push to ECR).

---

## 2. GitHub setup (one-time)

- **Enable Actions**: repo → Settings → Actions → General → Allow all actions.
- **Add secrets**: Settings → Secrets and variables → Actions → New repository secret.  
  Add these (values from your AWS setup):

| Secret | Description |
|--------|-------------|
| `AWS_ACCESS_KEY_ID` | IAM user access key (used by Actions to call AWS) |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |
| `AWS_REGION` | e.g. `us-east-1` |
| `ECR_REPOSITORY` | ECR repo name, e.g. `globalskiatlas-backend-k8s-pipeline` |
| `ECS_CLUSTER` | ECS cluster name, e.g. `globalskiatlas-backend-k8s` |
| `ECS_SUBNETS` | Comma-separated subnet IDs (e.g. `subnet-xxx,subnet-yyy`) |
| `ECS_SECURITY_GROUP` | Security group ID (outbound HTTPS allowed) |
| `S3_BUCKET` | Bucket for pipeline output, e.g. `globalskiatlas-backend-k8s-output` |

- The IAM user for the keys needs: ECR push, ECS run-task, and (if you run tasks) `iam:PassRole` for the ECS task and execution roles. See [AWS_ECS_DEPLOYMENT.md](AWS_ECS_DEPLOYMENT.md) for the exact policy.

---

## 3. AWS one-time setup

Do this once per account/region. Details are in [AWS_ECS_DEPLOYMENT.md](AWS_ECS_DEPLOYMENT.md); summary:

1. **S3**: Create bucket (e.g. `globalskiatlas-backend-k8s-output`).
2. **ECR**: Create repository (e.g. `globalskiatlas-backend-k8s-pipeline`).
3. **IAM**:  
   - Execution role for ECS (e.g. `AmazonECSTaskExecutionRolePolicy`).  
   - Task role with `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on the bucket.  
   - IAM user for GitHub Actions with ECR push + ECS run-task (+ PassRole) as above.
4. **ECS**: Create cluster (e.g. `globalskiatlas-backend-k8s`).
5. **CloudWatch**: Log group `/ecs/globalskiatlas-backend-k8s-pipeline`.
6. **Task definitions**: Register the JSONs under `aws/` (update account ID and role ARNs if needed):
   ```bash
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-small.json --region us-east-1
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-medium.json --region us-east-1
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-large.json --region us-east-1
   aws ecs register-task-definition --cli-input-json file://aws/ecs-task-pipeline-xlarge.json --region us-east-1
   ```

Use the same names/IDs in the GitHub secrets (ECR repo, cluster, S3 bucket, subnets, security group).

---

## 4. What runs when

- **Push to `main`**:  
  - Workflow **Deploy pipeline to AWS** runs.  
  - Builds image from `Dockerfile.aws`, pushes to `ECR_REPOSITORY` with tag `github.sha` and `latest`.  
  - Does **not** start an ECS task.

- **Manual "Run ECS task"**:  
  - Actions → **Deploy pipeline to AWS** → **Run workflow**.  
  - Set **Run ECS task after push** = true, choose **Region** (e.g. iceland, europe).  
  - After build-and-push, the workflow runs an ECS task for that region; the task runs the pipeline and uploads Parquet/CSV to `s3://S3_BUCKET/<region>/YYYY-MM/`.

---

## 5. Checklist to establish the workflow

- [ ] AWS: S3 bucket, ECR repo, IAM roles, ECS cluster, log group, task definitions registered.
- [ ] GitHub: All 8 secrets set (AWS keys, ECR_REPOSITORY, ECS_CLUSTER, ECS_SUBNETS, ECS_SECURITY_GROUP, S3_BUCKET, AWS_REGION).
- [ ] Local: Push to `main` and confirm Actions run and image appears in ECR.
- [ ] Optional: Trigger "Run ECS task" for `iceland` and confirm output in S3 and logs in CloudWatch.

Once this is done, the backend has a defined path: **local PC → GitHub → GitHub Actions → AWS (ECR + optional ECS → S3)**.
