# Table: gcp_firebase_app

Query all Firebase apps (Android, iOS, and Web) across platforms in a unified view. Use the `platform` column to distinguish app types. For platform-specific fields like `bundle_id` or `package_name`, query the dedicated tables `gcp_firebase_ios_app` or `gcp_firebase_android_app`.

## Examples

### List all Firebase apps across all platforms

```sql
select
  display_name,
  app_id,
  platform,
  namespace,
  state
from
  gcp_firebase_app;
```

### Count apps by platform

```sql
select
  platform,
  count(*) as app_count
from
  gcp_firebase_app
where
  state = 'ACTIVE'
group by
  platform;
```

### List deleted apps that are still within the recovery window

```sql
select
  display_name,
  app_id,
  platform,
  expire_time
from
  gcp_firebase_app
where
  state = 'DELETED';
```

### Find apps with no API key associated

```sql
select
  display_name,
  app_id,
  platform
from
  gcp_firebase_app
where
  api_key_id is null
  and state = 'ACTIVE';
```
