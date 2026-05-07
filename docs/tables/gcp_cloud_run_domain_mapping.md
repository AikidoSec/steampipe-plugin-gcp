---
title: "Steampipe Table: gcp_cloud_run_domain_mapping - Query GCP Cloud Run Domain Mappings using SQL"
description: "Allows users to query GCP Cloud Run Domain Mappings, providing insights into custom domain configurations mapped to Cloud Run services."
folder: "Cloud Run"
---

# Table: gcp_cloud_run_domain_mapping - Query GCP Cloud Run Domain Mappings using SQL

A Cloud Run Domain Mapping associates a custom domain with a Cloud Run service. When active, it provisions a TLS certificate and provides the DNS records needed to route traffic from the domain to the service.

## Table Usage Guide

The `gcp_cloud_run_domain_mapping` table provides insights into custom domain configurations within Google Cloud Platform (GCP). Use it to audit which domains are mapped to which services, inspect certificate modes, and retrieve the DNS records required for domain verification.

## Examples

### Basic info
List all domain mappings with their target service and status URL.

```sql+postgres
select
  name,
  route_name,
  location,
  certificate_mode,
  url,
  create_time
from
  gcp_cloud_run_domain_mapping;
```

```sql+sqlite
select
  name,
  route_name,
  location,
  certificate_mode,
  url,
  create_time
from
  gcp_cloud_run_domain_mapping;
```

### List mappings using automatic certificate provisioning
```sql+postgres
select
  name,
  route_name,
  location,
  url
from
  gcp_cloud_run_domain_mapping
where
  certificate_mode = 'AUTOMATIC';
```

```sql+sqlite
select
  name,
  route_name,
  location,
  url
from
  gcp_cloud_run_domain_mapping
where
  certificate_mode = 'AUTOMATIC';
```

### List mappings configured with force override
```sql+postgres
select
  name,
  route_name,
  location
from
  gcp_cloud_run_domain_mapping
where
  force_override;
```

```sql+sqlite
select
  name,
  route_name,
  location
from
  gcp_cloud_run_domain_mapping
where
  force_override;
```

### Retrieve DNS records for active domain mappings
```sql+postgres
select
  name,
  location,
  r ->> 'name' as record_name,
  r ->> 'type' as record_type,
  r ->> 'rrdata' as record_value
from
  gcp_cloud_run_domain_mapping,
  jsonb_array_elements(resource_records) as r;
```

```sql+sqlite
select
  name,
  location,
  json_extract(r.value, '$.name') as record_name,
  json_extract(r.value, '$.type') as record_type,
  json_extract(r.value, '$.rrdata') as record_value
from
  gcp_cloud_run_domain_mapping,
  json_each(resource_records) as r;
```

### Get a specific domain mapping
```sql+postgres
select
  name,
  route_name,
  certificate_mode,
  url,
  conditions
from
  gcp_cloud_run_domain_mapping
where
  name = 'example.com'
  and location = 'us-central1';
```

```sql+sqlite
select
  name,
  route_name,
  certificate_mode,
  url,
  conditions
from
  gcp_cloud_run_domain_mapping
where
  name = 'example.com'
  and location = 'us-central1';
```
