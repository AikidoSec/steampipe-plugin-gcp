# Table: gcp_firebase_android_app

Query Firebase Android apps registered in your Firebase project.

## Examples

### List all Android apps

```sql
select
  name,
  display_name,
  app_id,
  package_name,
  state
from
  gcp_firebase_android_app;
```

### List active Android apps

```sql
select
  name,
  display_name,
  package_name
from
  gcp_firebase_android_app
where
  state = 'ACTIVE';
```

### Get SHA certificate hashes for each Android app

```sql
select
  display_name,
  package_name,
  jsonb_array_elements_text(sha1_hashes::jsonb) as sha1_hash
from
  gcp_firebase_android_app
where
  sha1_hashes is not null;
```

### Find Android apps with no SHA certificate hashes registered

```sql
select
  display_name,
  package_name,
  app_id
from
  gcp_firebase_android_app
where
  sha1_hashes is null
  and state = 'ACTIVE';
```
