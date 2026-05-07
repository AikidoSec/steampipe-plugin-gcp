# Table: gcp_firebase_web_app

Query Firebase web apps registered in your Firebase project.

## Examples

### List all web apps

```sql
select
  name,
  display_name,
  app_id,
  web_id,
  state
from
  gcp_firebase_web_app;
```

### List active web apps with their hosted URLs

```sql
select
  display_name,
  app_id,
  jsonb_array_elements_text(app_urls::jsonb) as url
from
  gcp_firebase_web_app
where
  state = 'ACTIVE'
  and app_urls is not null;
```

### Find web apps with no hosted URLs configured

```sql
select
  display_name,
  app_id
from
  gcp_firebase_web_app
where
  app_urls is null
  and state = 'ACTIVE';
```

### Get the API key associated with each web app

```sql
select
  display_name,
  app_id,
  api_key_id
from
  gcp_firebase_web_app
where
  state = 'ACTIVE';
```
