--- Cookies in Jar and local storage
SELECT storage_type, count(*) as export_data
                                                FROM `hbbtv.tv_cookie_store`
                                                GROUP BY storage_type
                                                ORDER BY storage_type;

--- Distinct cookies in local storage and cookie jar
SELECT foo.storage_type, count(*) as count FROM (SELECT distinct cs.name, cs.path, cs.storage_type FROM `hbbtv-research.hbbtv.tv_cookie_store` cs) as foo group by foo.storage_type;

--- Distinct cookies in http
SELECT distinct c.name, c.origin, c.path FROM `hbbtv-research.hbbtv.cookies` c;

--- Cookies, which are in Cookie Jar/Local Storage AND http cookies
SELECT distinct c.name, c.path FROM `hbbtv-research.hbbtv.cookies` c, (SELECT distinct cs.name, cs.path, cs.storage_type FROM `hbbtv-research.hbbtv.tv_cookie_store` cs) AS foo WHERE c.name = foo.name and c.path = foo.path

--- AVG, MIN, MAX, SD, Cookies
SELECT
  MAX(bar.count) AS max,
  MIN(bar.count) AS MIN,
  AVG(bar.count) AS AVG,
  STDDEV_POP(bar.count) AS SD
FROM (
  SELECT
    foo.channelid,
    foo.scan_profile,
    COUNT(*) AS count
  FROM (
    SELECT
      DISTINCT c.origin,
      c.scan_profile,
      req.channelid,
      COUNT(*) AS count
    FROM
      `hbbtv-research.hbbtv.requests` req,
      `hbbtv-research.hbbtv.cookies` c
    WHERE
      req.request_id=c.request_id
      AND req.scan_profile=c.scan_profile
      AND NOT c.duplicate
      #AND req.is_third_party
    GROUP BY
      c.origin,
      c.scan_profile,
      req.channelid
    ORDER BY
      req.channelid,
      count DESC) AS foo
  GROUP BY
    foo.channelid,
    foo.scan_profile
  ORDER BY
    foo.scan_profile) AS bar

--- Successfully classified cookies
SELECT
  SUM(foo.count)
FROM (
  SELECT
    purpose,
    COUNT(*) as count
  FROM
    `hbbtv-research.hbbtv.cookies`
  GROUP BY
    purpose) AS foo;

SELECT
    purpose,
    COUNT(*)
  FROM
    `hbbtv-research.hbbtv.cookies`
  GROUP BY
    purpose;

--- MEAN, MIN, MAX, SD distinct parties that set cookies on a channel
SELECT
  MIN(bar.count) AS min,
  MAX(bar.count) AS max,
  STDDEV_POP(bar.count) AS sd,
  AVG(bar.count) AS avg
FROM (
  SELECT
    foo.origin AS origin,
    COUNT(*) AS count
  FROM (
    SELECT
      DISTINCT c.origin,
      req.channelname
    FROM
      `hbbtv-research.hbbtv.requests` req,
      `hbbtv-research.hbbtv.cookies` c
    WHERE
      req.request_id=c.request_id
      AND req.scan_profile=c.scan_profile
      AND NOT c.duplicate
      AND req.is_third_party) AS foo
  GROUP BY
    foo.origin
  ORDER BY
    COUNT(*) DESC) AS bar;

--- cookie syncing
SELECT
  DISTINCT c. origin,
  c.name,
  c.value,
  c.scan_profile,
  c.request_id
FROM
  `hbbtv.cookies` c
WHERE
  BYTE_LENGTH(c.value) > 10
  AND BYTE_LENGTH(c.value) < 25
  AND NOT duplicate;


  SELECT
  DISTINCT c. origin,
  c.name,
  c.value,
  c.scan_profile,
  c.request_id
FROM
  `hbbtv.cookies` c;

 SELECT
  DISTINCT c. origin,
  c.name,
  c.value,
  c.scan_profile,
  c.request_id
FROM
  `hbbtv.cookies` c
WHERE not duplicate;

SELECT
  DISTINCT c. origin,
  c.name,
  c.value,
  c.scan_profile,
  c.request_id
FROM
  `hbbtv.cookies` c
WHERE
  BYTE_LENGTH(c.value) > 10
  AND BYTE_LENGTH(c.value) < 25;

--- timestampts in syncing
SELECT is_timestamp, count(*) FROM `hbbtv-research.hbbtv.Cookie_Syncing` group by is_timestamp

--- channels cookie syncing
SELECT
  DISTINCT *
FROM (
  SELECT
    DISTINCT(source_channelname)
  FROM
    `hbbtv-research.hbbtv.Cookie_Syncing`
  WHERE
    NOT is_timestamp
  UNION ALL
  SELECT
    DISTINCT(destination_channelname)
  FROM
    `hbbtv-research.hbbtv.Cookie_Syncing`
  WHERE
    NOT is_timestamp);

--- cookie syncing
--- Syncing activities
SELECT cs.etld AS src_etld, req.etld AS dst_etld, cs.cookie_value, cs.cookie_name FROM `hbbtv-research.hbbtv.Cookie_Syncing` cs, `hbbtv-research.hbbtv.requests` req WHERE not cs.is_timestamp AND cs.destination_request_id = req.request_id and cs.scan_profile = req.scan_profile AND cs.etld != req.etld;

--- Per Profile
SELECT foo.scan_profile, count(*) FROM (SELECT cs.etld AS src_etld, req.etld AS dst_etld, cs.cookie_value, cs.cookie_name, cs.scan_profile FROM `hbbtv-research.hbbtv.Cookie_Syncing` cs, `hbbtv-research.hbbtv.requests` req WHERE not cs.is_timestamp AND cs.destination_request_id = req.request_id and cs.scan_profile = req.scan_profile AND cs.etld != req.etld) AS foo GROUP BY foo.scan_profile;

--- Per Party
SELECT foo.src_etld, count(*) FROM (SELECT cs.etld AS src_etld, req.etld AS dst_etld, cs.cookie_value, cs.cookie_name, cs.scan_profile FROM `hbbtv-research.hbbtv.Cookie_Syncing` cs, `hbbtv-research.hbbtv.requests` req WHERE not cs.is_timestamp AND cs.destination_request_id = req.request_id and cs.scan_profile = req.scan_profile AND cs.etld != req.etld) AS foo GROUP BY foo.src_etld;

--- Per cookie
SELECT foo.src_etld, foo.cookie_name, count(*) FROM (SELECT cs.etld AS src_etld, req.etld AS dst_etld, cs.cookie_value, cs.cookie_name, cs.scan_profile FROM `hbbtv-research.hbbtv.Cookie_Syncing` cs, `hbbtv-research.hbbtv.requests` req WHERE not cs.is_timestamp AND cs.destination_request_id = req.request_id and cs.scan_profile = req.scan_profile AND cs.etld != req.etld) AS foo GROUP BY foo.src_etld, foo.cookie_name;

--- Related to channel (dst)
SELECT foo.channelname, count(*) as occurency FROM (SELECT cs.etld AS src_etld, req.etld AS dst_etld, cs.cookie_value, cs.cookie_name, cs.scan_profile, req.channelname FROM `hbbtv-research.hbbtv.Cookie_Syncing` cs, `hbbtv-research.hbbtv.requests` req WHERE not cs.is_timestamp AND cs.destination_request_id = req.request_id and cs.scan_profile = req.scan_profile AND cs.etld != req.etld) AS foo GROUP BY foo.channelname;

--- Distribution of syncing on profiles
SELECT scan_profile, count(*) FROM `hbbtv-research.hbbtv.Cookie_Syncing` WHERE not is_timestamp group by scan_profile;

--- Distribution of origins
SELECT cookie_origin, count(*) as count FROM `hbbtv-research.hbbtv.Cookie_Syncing` WHERE not is_timestamp group by cookie_origin order by count desc;

--- Distribution of names
SELECT cookie_name, count(*) as count FROM `hbbtv-research.hbbtv.Cookie_Syncing` WHERE not is_timestamp group by cookie_name order by count desc;

#

