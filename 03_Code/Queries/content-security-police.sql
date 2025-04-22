/* requests with CSP*/ -- 5.4.1 number OF csp requests
SELECT
COUNT(*) ct,
COUNT(*)/ (
SELECT
  COUNT(*)
FROM
  `hbbtv-research.hbbtv.responses`) pct
FROM
`hbbtv-research.hbbtv.responses`
WHERE
LOWER(headers) LIKE "%content-security-policy%"; /*,
-,
-,
-,
-- 5.4.2 are requests csp third-partys? */
SELECT
COUNT(*) ct,
COUNT(*)/ (
SELECT
  COUNT(*)
FROM
  `hbbtv-research.hbbtv.responses`
WHERE
  LOWER(headers) LIKE "%content-security-policy%")
FROM
hbbtv.responses
WHERE
LOWER(headers) LIKE "%content-security-policy%"
AND is_first_party; /*,
-,
-,
-,
-- 5.4.2 requests
WITH
x-content-type-options ? */
SELECT
COUNT(*) ct,
COUNT(*)/ (
SELECT
  COUNT(*)
FROM
  `hbbtv-research.hbbtv.responses` )
FROM
hbbtv.responses
WHERE
LOWER(headers) LIKE "%x-content-type-options%" ;
