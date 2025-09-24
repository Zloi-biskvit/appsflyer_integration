CREATE TABLE IF NOT EXISTS af_raw_installs (
  event_date date,
  app_id text,
  app_name text,
  platform text,
  media_source text,
  campaign text,
  adset text,
  ad text,
  country text,
  impressions bigint,
  clicks bigint,
  installs bigint,
  cost numeric,
  revenue numeric,
  d1_retained bigint,
  d7_retained bigint
);

CREATE TABLE IF NOT EXISTS af_metrics_daily (
  event_date date,
  app_id text,
  media_source text,
  campaign text,
  country text,
  impressions bigint,
  clicks bigint,
  installs bigint,
  cost numeric,
  revenue numeric,
  cpi numeric,
  ctr numeric,
  cvr numeric,
  roas numeric,
  arpu numeric,
  d1_retention numeric,
  d7_retention numeric,
  PRIMARY KEY (event_date, app_id, media_source, campaign, country)
);
