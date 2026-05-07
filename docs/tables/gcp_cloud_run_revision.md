---
title: "Steampipe Table: gcp_cloud_run_revision - Query GCP Cloud Run Revisions using SQL"
description: "Allows users to query GCP Cloud Run Revisions, providing insights into the immutable snapshots of code and configuration deployed to a Cloud Run service."
folder: "Cloud Run"
---

# Table: gcp_cloud_run_revision - Query GCP Cloud Run Revisions using SQL

A Cloud Run Revision is an immutable snapshot of code and configuration. Every deploy to a Cloud Run service creates a new revision. Revisions can be scaled independently and traffic can be split across multiple revisions.

## Table Usage Guide

The `gcp_cloud_run_revision` table provides insights into Cloud Run revisions within Google Cloud Platform (GCP). Use it to inspect revision configuration, container settings, scaling, and VPC access. You can filter by `service_name` to scope results to a specific service.

## Examples

### Basic info
List all revisions with their parent service, creation time, and reconciliation status.

```sql+postgres
select
  name,
  service_name,
  location,
  create_time,
  reconciling,
  observed_generation
from
  gcp_cloud_run_revision;
```

```sql+sqlite
select
  name,
  service_name,
  location,
  create_time,
  reconciling,
  observed_generation
from
  gcp_cloud_run_revision;
```

### List revisions for a specific service
```sql+postgres
select
  name,
  create_time,
  service_account,
  max_instance_request_concurrency,
  timeout
from
  gcp_cloud_run_revision
where
  service_name = 'my-service';
```

```sql+sqlite
select
  name,
  create_time,
  service_account,
  max_instance_request_concurrency,
  timeout
from
  gcp_cloud_run_revision
where
  service_name = 'my-service';
```

### Get a specific revision
```sql+postgres
select
  name,
  service_name,
  location,
  launch_stage,
  execution_environment,
  service_account
from
  gcp_cloud_run_revision
where
  name = 'my-service-00001-abc'
  and service_name = 'my-service'
  and location = 'us-central1';
```

```sql+sqlite
select
  name,
  service_name,
  location,
  launch_stage,
  execution_environment,
  service_account
from
  gcp_cloud_run_revision
where
  name = 'my-service-00001-abc'
  and service_name = 'my-service'
  and location = 'us-central1';
```

### List revisions using a customer-managed encryption key (CMEK)
```sql+postgres
select
  name,
  service_name,
  location,
  encryption_key
from
  gcp_cloud_run_revision
where
  encryption_key is not null;
```

```sql+sqlite
select
  name,
  service_name,
  location,
  encryption_key
from
  gcp_cloud_run_revision
where
  encryption_key is not null;
```

### List revisions that are still reconciling
```sql+postgres
select
  name,
  service_name,
  location,
  generation,
  observed_generation
from
  gcp_cloud_run_revision
where
  reconciling;
```

```sql+sqlite
select
  name,
  service_name,
  location,
  generation,
  observed_generation
from
  gcp_cloud_run_revision
where
  reconciling;
```
