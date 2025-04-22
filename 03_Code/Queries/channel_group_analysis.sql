--- Number of channels per category
SELECT
  channel_category,
  COUNT(*) as number_of_channels
FROM
  `hbbtv-research.hbbtv.channellist`
  GROUP BY channel_category
order by number_of_channels;

--- MIN,MAX,MEAN,SD for each channel
SELECT
  MIN(number_of_channels) min,
  MAX(number_of_channels) max,
  AVG(number_of_channels) mean,
  STDDEV(number_of_channels) SD
FROM (
  SELECT
    channel_category,
    COUNT(*) AS number_of_channels
  FROM
    `hbbtv-research.hbbtv.channellist`
  GROUP BY
    channel_category
  ORDER BY
    number_of_channels);

--- Percentage channel per category
    WITH
  total_channels AS (
  SELECT
    SUM(number_of_channels) AS total_channel
  FROM (
    SELECT
      channel_category,
      COUNT(*) AS number_of_channels
    FROM
      `hbbtv-research.hbbtv.channellist`
    GROUP BY
      channel_category
    ORDER BY
      number_of_channels ) )
SELECT
  channel_category,
  (number_of_channels / total_channel) * 100 AS channel_perc
FROM (
  SELECT
    channel_category,
    COUNT(*) AS number_of_channels,
    total_channel
  FROM
    `hbbtv-research.hbbtv.channellist`,
    total_channels
  GROUP BY
    channel_category,
    total_channel
  ORDER BY
    number_of_channels);


--- Number of tracker for each group
SELECT
channel_category,
  COUNT(*) number_of_trackers
FROM
  `hbbtv-research.hbbtv.channellist` AS chan,
  `hbbtv-research.hbbtv.requests` AS req,
  `hbbtv-research.hbbtv.responses` AS res
WHERE
  req.channelid = chan.programId
  AND ( (req.is_known_tracker)
    OR (res.type LIKE '%image%'
      AND CAST(res.size AS int) < 45
      AND CAST(res.status AS int) = 200)
    OR ((LOWER(res.response) LIKE '%webgl%'
        OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
        OR LOWER(res.response) LIKE '%supercookie%')) )
  AND res.request_id = req.request_id
  AND res.scan_profile = req.scan_profile
  AND NOT req.is_iptv
  GROUP BY channel_category
  ORDER BY number_of_trackers;

--- Number of tracker for each group (min,max,sd,mean)
SELECT
  MIN(number_of_trackers) min,
  MAX(number_of_trackers) max,
  AVG(number_of_trackers) mean,
  STDDEV(number_of_trackers) SD
FROM (
SELECT
channel_category,
  COUNT(*) number_of_trackers
FROM
  `hbbtv-research.hbbtv.channellist` AS chan,
  `hbbtv-research.hbbtv.requests` AS req,
  `hbbtv-research.hbbtv.responses` AS res
WHERE
  req.channelid = chan.programId
  AND ( (req.is_known_tracker)
    OR (res.type LIKE '%image%'
      AND CAST(res.size AS int) < 45
      AND CAST(res.status AS int) = 200)
    OR ((LOWER(res.response) LIKE '%webgl%'
        OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
        OR LOWER(res.response) LIKE '%supercookie%')) )
  AND res.request_id = req.request_id
  AND res.scan_profile = req.scan_profile
  AND NOT req.is_iptv
  GROUP BY channel_category
  ORDER BY number_of_trackers);

--- Percentage of categories by tracker
WITH
  total_trackers AS (
  SELECT
    SUM(number_of_trackers) num_total_trackers
  FROM (
    SELECT
      channel_category,
      COUNT(*) number_of_trackers
    FROM
      `hbbtv-research.hbbtv.channellist` AS chan,
      `hbbtv-research.hbbtv.requests` AS req,
      `hbbtv-research.hbbtv.responses` AS res
    WHERE
      req.channelid = chan.programId
      AND ( (req.is_known_tracker)
        OR (res.type LIKE '%image%'
          AND CAST(res.size AS int) < 45
          AND CAST(res.status AS int) = 200)
        OR ((LOWER(res.response) LIKE '%webgl%'
            OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
            OR LOWER(res.response) LIKE '%supercookie%')) )
      AND res.request_id = req.request_id
      AND res.scan_profile = req.scan_profile
      AND NOT req.is_iptv
    GROUP BY
      channel_category
    ORDER BY
      number_of_trackers) )
SELECT
  channel_category,
  number_of_trackers,
  (number_of_trackers / num_total_trackers) * 100 AS percantage_from_total
FROM (
  SELECT
    channel_category,
    COUNT(*) number_of_trackers,
    num_total_trackers
  FROM
    `hbbtv-research.hbbtv.channellist` AS chan,
    `hbbtv-research.hbbtv.requests` AS req,
    `hbbtv-research.hbbtv.responses` AS res,
    total_trackers
  WHERE
    req.channelid = chan.programId
    AND ( (req.is_known_tracker)
      OR (res.type LIKE '%image%'
        AND CAST(res.size AS int) < 45
        AND CAST(res.status AS int) = 200)
      OR ((LOWER(res.response) LIKE '%webgl%'
          OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
          OR LOWER(res.response) LIKE '%supercookie%')) )
    AND res.request_id = req.request_id
    AND res.scan_profile = req.scan_profile
    AND NOT req.is_iptv
  GROUP BY
    channel_category,
    num_total_trackers
  ORDER BY
    number_of_trackers);

--- Mean Tracker per Channel Category
SELECT
  channel_category,
  AVG(trackers) AS mean_trackers_per_category
FROM (
  SELECT
    req.channelid,
    channel_category,
    COUNT(*) AS trackers
  FROM
    `hbbtv-research.hbbtv.channellist` AS chan,
    `hbbtv-research.hbbtv_backup.requests` AS req,
    `hbbtv-research.hbbtv.responses` AS res
  WHERE
    req.channelid = chan.programId
    AND ( (req.is_known_tracker)
      OR (res.type LIKE '%image%'
        AND CAST(res.size AS int) < 45
        AND CAST(res.status AS int) = 200)
      OR ((LOWER(res.response) LIKE '%webgl%'
          OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
          OR LOWER(res.response) LIKE '%supercookie%')) )
    AND res.request_id = req.request_id
    AND res.scan_profile = req.scan_profile
    AND NOT req.is_iptv
  GROUP BY
    chan.channel_category,
    req.channelid)
GROUP BY
  channel_category
ORDER BY
  mean_trackers_per_category;