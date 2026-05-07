---
title: "Steampipe Table: gcp_cloud_run_worker_pool - Query GCP Cloud Run Worker Pools using SQL"
description: "Allows users to query GCP Cloud Run Worker Pools, providing insights into long-running worker configurations including scaling, instance splits, and IAM policies."
folder: "Cloud Run"
---

# Table: gcp_cloud_run_worker_pool - Query GCP Cloud Run Worker Pools using SQL

A Cloud Run Worker Pool is a resource for running long-lived, non-request-driven workloads on Cloud Run. Unlike services, worker pools are not invoked via HTTP — they run continuously and are scaled independently using instance splits across revisions.

## Table Usage Guide

The `gcp_cloud_run_worker_pool` table provides insights into Cloud Run Worker Pools within Google Cloud Platform (GCP). Use it to inspect worker pool configuration, scaling settings, revision splits, and IAM policies.

## Examples

### Basic info
List all worker pools with their current status and revision information.

```sql+postgres
select
  name,
  location,
  launch_stage,
  latest_ready_revision,
  latest_created_revision,
  reconciling,
  create_time
from
  gcp_cloud_run_worker_pool;
```

```sql+sqlite
select
  name,
  location,
  launch_stage,
  latest_ready_revision,
  latest_created_revision,
  reconciling,
  create_time
from
  gcp_cloud_run_worker_pool;
```

### List worker pools that are currently reconciling
```sql+postgres
select
  name,
  location,
  generation,
  observed_generation,
  latest_created_revision
from
  gcp_cloud_run_worker_pool
where
  reconciling;
```

```sql+sqlite
select
  name,
  location,
  generation,
  observed_generation,
  latest_created_revision
from
  gcp_cloud_run_worker_pool
where
  reconciling;
```

### List worker pools using a customer-managed encryption key (CMEK)
```sql+postgres
select
  name,
  location,
  template -> 'encryptionKey' as encryption_key
from
  gcp_cloud_run_worker_pool
where
  template ->> 'encryptionKey' is not null;
```

```sql+sqlite
select
  name,
  location,
  json_extract(template, '$.encryptionKey') as encryption_key
from
  gcp_cloud_run_worker_pool
where
  json_extract(template, '$.encryptionKey') is not null;
```

### Inspect instance split configuration
```sql+postgres
select
  name,
  location,
  s ->> 'revision' as revision,
  s ->> 'percent' as percent,
  s ->> 'type' as split_type
from
  gcp_cloud_run_worker_pool,
  jsonb_array_elements(instance_splits) as s;
```

```sql+sqlite
select
  name,
  location,
  json_extract(s.value, '$.revision') as revision,
  json_extract(s.value, '$.percent') as percent,
  json_extract(s.value, '$.type') as split_type
from
  gcp_cloud_run_worker_pool,
  json_each(instance_splits) as s;
```

### Get IAM policy bindings for worker pools
```sql+postgres
select
  name,
  location,
  b ->> 'role' as role,
  m as member
from
  gcp_cloud_run_worker_pool,
  jsonb_array_elements(iam_policy -> 'bindings') as b,
  jsonb_array_elements_text(b -> 'members') as m;
```

```sql+sqlite
select
  name,
  location,
  json_extract(b.value, '$.role') as role,
  m.value as member
from
  gcp_cloud_run_worker_pool,
  json_each(json_extract(iam_policy, '$.bindings')) as b,
  json_each(json_extract(b.value, '$.members')) as m;
```

### Get a specific worker pool
```sql+postgres
select
  name,
  location,
  description,
  launch_stage,
  scaling,
  template
from
  gcp_cloud_run_worker_pool
where
  name = 'my-worker-pool'
  and location = 'us-central1';
```

```sql+sqlite
select
  name,
  location,
  description,
  launch_stage,
  scaling,
  template
from
  gcp_cloud_run_worker_pool
where
  name = 'my-worker-pool'
  and location = 'us-central1';
```
