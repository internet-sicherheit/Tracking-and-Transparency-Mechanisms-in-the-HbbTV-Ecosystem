--- Overview
SELECT
  bar.profile,
  MAX(bar.count) AS max,
  MIN(bar.count) AS MIN,
  AVG(bar.count) AS AVG,
  STDDEV_POP(bar.count) AS SD,
  SUM(bar.count) AS third_party_cookies
FROM (
  SELECT
    foo.scan_profile AS profile,
    foo.channelname AS channelname,
    #foo.origin AS origin,
    COUNT(*) AS count
  FROM (
    SELECT
      DISTINCT c.origin,
      c.scan_profile,
      req.channelname,
    FROM
      `hbbtv-research.hbbtv.requests` req,
      `hbbtv-research.hbbtv.cookies` c
    WHERE
      req.request_id=c.request_id
      AND req.scan_profile=c.scan_profile
      AND NOT c.duplicate
      AND req.is_third_party) AS foo
  GROUP BY
    foo.channelname,
    foo.scan_profile
  ORDER BY
    COUNT(*) DESC) AS bar
GROUP BY
  bar.profile
ORDER BY
  profile;

---MEAN, MIN, MAX, SD tp usage
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
      AND req.is_third_party
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
    foo.scan_profile) AS bar;

---- top third parties
SELECT
  foo.origin AS origin,
  COUNT(*) AS count
FROM (
  SELECT
    DISTINCT c.origin,
    req.channelid
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
  COUNT(*) DESC;

--- MEAN, MIN, MAX, SD tp usage with atleast one tracker
SELECT
  MAX(bar.count) AS max,
  MIN(bar.count) AS MIN,
  AVG(bar.count) AS AVG,
  STDDEV_POP(bar.count) AS SD,
FROM (
  SELECT
    foo.origin AS origin,
    COUNT(*) AS count
  FROM (
    SELECT
      DISTINCT c.origin,
      req.channelid
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
    COUNT(*) DESC) AS bar
WHERE
  bar.count > 1;

--- cookies set by third parties
WITH
  third_parties AS (
  SELECT
    bar.origin
  FROM (
    SELECT
      foo.origin AS origin,
      COUNT(*) AS count
    FROM (
      SELECT
        DISTINCT c.origin,
        req.channelid
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
      COUNT(*) DESC) AS bar
  WHERE
    bar.count > 1)
SELECT
  purpose,
  COUNT(*) AS count,
  COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS percentage
FROM
  `hbbtv-research.hbbtv.cookies` c,
  third_parties tp
WHERE
  c.origin = tp.origin
GROUP BY
  purpose;

--- distinct parties
SELECT count(distinct origin) FROM `hbbtv-research.hbbtv.cookies`

--- easylist detection
WITH
  is_tracking AS (
  SELECT
    COUNT(*) AS easylist_blocked
  FROM
    `hbbtv-research.hbbtv.requests`
  WHERE
    is_known_tracker),
  total_traffic AS
(SELECT
  COUNT(*) AS traffic
FROM
  `hbbtv-research.hbbtv.requests`)
SELECT is_tracking.easylist_blocked/total_traffic.traffic * 100 AS percentage_of_blocked_by_easylist
FROM
  is_tracking,
  total_traffic;

--- pi-hole detection
WITH
  is_tracking AS (
  SELECT
    COUNT(*) AS pi_hole_blocked
  FROM
    `hbbtv-research.hbbtv.requests`
  WHERE
    pi_hole_blocked),
  total_traffic AS
(SELECT
  COUNT(*) AS traffic
FROM
  `hbbtv-research.hbbtv.requests`)
SELECT is_tracking.pi_hole_blocked/total_traffic.traffic * 100 AS percentage_of_blocked_by_pi_hole
FROM
  is_tracking,
  total_traffic;




