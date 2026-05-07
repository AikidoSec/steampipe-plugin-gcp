# Table: gcp_firebase_ios_app

Query Firebase iOS apps registered in your Firebase project.

## Examples

### List all iOS apps

```sql
select
  name,
  display_name,
  app_id,
  bundle_id,
  state
from
  gcp_firebase_ios_app;
```

### List active iOS apps

```sql
select
  name,
  display_name,
  bundle_id,
  team_id
from
  gcp_firebase_ios_app
where
  state = 'ACTIVE';
```

### Find iOS apps missing an App Store ID

```sql
select
  display_name,
  bundle_id,
  app_id
from
  gcp_firebase_ios_app
where
  app_store_id is null
  and state = 'ACTIVE';
```

### Find iOS apps missing a Team ID

```sql
select
  display_name,
  bundle_id,
  app_id
from
  gcp_firebase_ios_app
where
  team_id is null
  and state = 'ACTIVE';
```
