# Table: gcp_firebase_project

Query Firebase projects associated with your GCP account. A Firebase project is a container for Firebase apps and resources.

## Examples

### List all Firebase projects

```sql
select
  name,
  display_name,
  project_id,
  project_number,
  state
from
  gcp_firebase_project;
```

### List active Firebase projects

```sql
select
  name,
  display_name,
  project_id,
  state
from
  gcp_firebase_project
where
  state = 'ACTIVE';
```

### Get default Firebase resources for a project

```sql
select
  project_id,
  resources -> 'hostingSite' as hosting_site,
  resources -> 'storageBucket' as storage_bucket,
  resources -> 'realtimeDatabaseInstance' as realtime_database_instance
from
  gcp_firebase_project
where
  project_id = 'my-project';
```
