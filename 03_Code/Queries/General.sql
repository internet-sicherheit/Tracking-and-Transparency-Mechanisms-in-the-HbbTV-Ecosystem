--- MEAN, MIN, MAX, SD channels per measurement run
SELECT
  MIN(foo.number_channels) AS min,
  MAX(foo.number_channels) AS max,
  AVG(foo.number_channels) AS AVG,
  STDDEV_POP(foo.number_channels) AS SD
FROM (
  SELECT
    scan_profile,
    COUNT (DISTINCT req.channelid) AS number_channels
  FROM
    `hbbtv-research.hbbtv.requests` AS req
  GROUP BY
    scan_profile) AS FOO ;

--- Distinct channels over the dataset
SELECT COUNT(DISTINCT channelId)
FROM `hbbtv-research.hbbtv.requests`;

--- Number of total requests
SELECT count(*) FROM `hbbtv.requests`


