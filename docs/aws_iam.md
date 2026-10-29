# AWS: bucket design and least-privilege IAM

**STATUS: guidance only. No bucket, user or role has been created. Nothing here was applied.**

## Bucket
One private bucket, e.g. `carelake-<your-initials>-<random>` in a region close to your Snowflake
account (same region avoids cross-region transfer fees). Turn on: Block Public Access, default
encryption (SSE-S3), versioning (optional), and a lifecycle rule expiring `quarantine/` and old
versions. Prefix layout is documented in `src/carelake/ingestion/s3_upload.py`.

## Two identities, two policies
1. **Uploader** (your laptop/CI, via short-lived credentials such as SSO or OIDC, not long-lived keys):
```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:PutObject","s3:GetObject"],
  "Resource":"arn:aws:s3:::<BUCKET>/<PREFIX>/*"},
 {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::<BUCKET>",
  "Condition":{"StringLike":{"s3:prefix":"<PREFIX>/*"}}}]}
```
The uploader also calls `HeadObject`, which needs `s3:GetObject`.
2. **Snowflake read role** (assumed by Snowflake through a storage integration, so no keys are shared):
read-only on `<PREFIX>/ingested/*` (`s3:GetObject`, `s3:GetObjectVersion`) plus `s3:ListBucket` limited to that prefix.
No write or delete permission, and no access to `quarantine/`.

## Rules
Never commit keys; prefer `aws sso login` locally and GitHub OIDC in CI. Rotate or delete anything
created for experiments. Run `aws s3 ls` only against this bucket.

## Costs (orders of magnitude; check current AWS pricing)
Synthetic data here is a few hundred KB, so S3 storage and requests should cost cents at most.
Larger risks are forgotten resources and cross-region transfer, not volume.
