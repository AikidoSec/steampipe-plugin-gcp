---
title: "Steampipe Table: gcp_cloud_run_execution - Query GCP Cloud Run Executions using SQL"
description: "Allows users to query GCP Cloud Run Executions, providing insights into the individual runs of Cloud Run Jobs including task counts, status, and timing information."
folder: "Cloud Run"
---

# Table: gcp_cloud_run_execution - Query GCP Cloud Run Executions using SQL

A Cloud Run Execution represents a single run of a Cloud Run Job. Each time a job is triggered, an execution is created that tracks the lifecycle of all tasks spawned by that run, including counts of succeeded, failed, running, and cancelled tasks.

## Table Usage Guide

The `gcp_cloud_run_execution` table provides insights into Cloud Run Job Executions within Google Cloud Platform (GCP). Use it to monitor job run history, inspect task outcomes, and audit execution configuration. Filter by `job_name` to scope results to a specific job.

## Examples

### Basic info
List all executions with their parent job, timing, and task counts.

```sql+postgres
select
  name,
  job_name,
  location,
  create_time,
  completion_time,
  task_count,
  succeeded_count,
  failed_count
from
  gcp_cloud_run_execution;
```

```sql+sqlite
select
  name,
  job_name,
  location,
  create_time,
  completion_time,
  task_count,
  succeeded_count,
  failed_count
from
  gcp_cloud_run_execution;
```

### List executions for a specific job
```sql+postgres
select
  name,
  create_time,
  start_time,
  completion_time,
  succeeded_count,
  failed_count,
  cancelled_count
from
  gcp_cloud_run_execution
where
  job_name = 'my-job';
```

```sql+sqlite
select
  name,
  create_time,
  start_time,
  completion_time,
  succeeded_count,
  failed_count,
  cancelled_count
from
  gcp_cloud_run_execution
where
  job_name = 'my-job';
```

### Find executions with failed tasks
```sql+postgres
select
  name,
  job_name,
  location,
  task_count,
  failed_count,
  retried_count
from
  gcp_cloud_run_execution
where
  failed_count > 0;
```

```sql+sqlite
select
  name,
  job_name,
  location,
  task_count,
  failed_count,
  retried_count
from
  gcp_cloud_run_execution
where
  failed_count > 0;
```

### List currently running executions
```sql+postgres
select
  name,
  job_name,
  location,
  running_count,
  parallelism,
  start_time
from
  gcp_cloud_run_execution
where
  running_count > 0;
```

```sql+sqlite
select
  name,
  job_name,
  location,
  running_count,
  parallelism,
  start_time
from
  gcp_cloud_run_execution
where
  running_count > 0;
```

### Get a specific execution
```sql+postgres
select
  name,
  job_name,
  location,
  task_count,
  succeeded_count,
  log_uri
from
  gcp_cloud_run_execution
where
  name = 'my-job-abc12'
  and job_name = 'my-job'
  and location = 'us-central1';
```

```sql+sqlite
select
  name,
  job_name,
  location,
  task_count,
  succeeded_count,
  log_uri
from
  gcp_cloud_run_execution
where
  name = 'my-job-abc12'
  and job_name = 'my-job'
  and location = 'us-central1';
```
