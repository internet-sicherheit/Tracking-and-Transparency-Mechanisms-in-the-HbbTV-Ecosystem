--- Manufacture leak
SELECT DISTINCT channelname
FROM `hbbtv-research.hbbtv.requests`
WHERE lower(queryString) LIKE '%lge%'
OR queryString LIKE '%43UK6300LLB%'
OR lower(postdata) LIKE '%lge%'
OR postdata LIKE '%43UK6300LLB%';

--- Configuration leak
SELECT channelname
FROM `hbbtv-research.hbbtv.requests`
WHERE postdata LIKE '%height%'
OR queryString LIKE '%height%'
OR postdata LIKE '%width%'
OR queryString LIKE '%width%'
OR postdata LIKE '%year%'
OR queryString LIKE '%year%'
or queryString LIKE '%WEBOS4.0 05.40.26%'
or postdata LIKE '%WEBOS4.0 05.40.26%'
or queryString LIKE '%W4_LM18A%'
or postdata LIKE '%W4_LM18A%';

--- Location Leak
SELECT channelname
FROM `hbbtv-research.hbbtv.requests`
WHERE postdata LIKE '"de"'
OR queryString LIKE '"de"'
OR postdata LIKE '%device_hour%'
OR queryString LIKE '%device_hour%'
OR postdata LIKE '%device_local_hour%'
OR queryString LIKE '%device_local_hour%';

--- Device leaks
SELECT etld
FROM `hbbtv-research.hbbtv.requests`
WHERE postdata LIKE '"de"'
OR queryString LIKE '"de"'
OR postdata LIKE '%device_hour%'
OR queryString LIKE '%device_hour%'
OR postdata LIKE '%device_local_hour%'
OR queryString LIKE '%device_local_hour%'
OR lower(queryString) LIKE '%lge%'
OR queryString LIKE '%43UK6300LLB%'
OR lower(postdata) LIKE '%lge%'
OR postdata LIKE '%43UK6300LLB%'
OR postdata LIKE '%height%'
OR queryString LIKE '%height%'
OR postdata LIKE '%width%'
OR queryString LIKE '%width%'
OR postdata LIKE '%year%'
OR queryString LIKE '%year%'
or queryString LIKE '%WEBOS4.0 05.40.26%'
or postdata LIKE '%WEBOS4.0 05.40.26%'
or queryString LIKE '%W4_LM18A%'
or postdata LIKE '%W4_LM18A%';

--- Genre leak
SELECT etld
FROM `hbbtv-research.hbbtv.requests`
WHERE lower(postdata) LIKE '%genre%'
OR lower(queryString) LIKE '%genre%'

OR lower(postdata) LIKE '%abenteuer%'
OR lower(queryString) LIKE '%abenteuer%'
OR lower(postdata) LIKE '%adventure%'
OR lower(queryString) LIKE '%adventure%'

OR lower(postdata) LIKE '%action%'
OR lower(queryString) LIKE '%action%'

OR lower(postdata) LIKE '%animation%'
OR lower(queryString) LIKE '%animation%'

OR lower(postdata) LIKE '%dokumentation%'
OR lower(queryString) LIKE '%dokumentation%'
OR lower(postdata) LIKE '%documentation%'
OR lower(queryString) LIKE '%documentation%'

OR lower(postdata) LIKE '%drama%'
OR lower(queryString) LIKE '%drama%'

OR lower(postdata) LIKE '%erotik%'
OR lower(queryString) LIKE '%erotik%'
OR lower(postdata) LIKE '%eroticism%'
OR lower(queryString) LIKE '%eroticism%'

OR lower(postdata) LIKE '%familie%'
OR lower(queryString) LIKE '%familie%'
OR lower(postdata) LIKE '%family%'
OR lower(queryString) LIKE '%family%'

OR lower(postdata) LIKE '%fantasy%'
OR lower(queryString) LIKE '%fantasy%'

OR lower(postdata) LIKE '%horror%'
OR lower(queryString) LIKE '%horror%'

OR lower(postdata) LIKE '%kinder%'
OR lower(queryString) LIKE '%kinder%'
OR lower(postdata) LIKE '%kids%'
OR lower(queryString) LIKE '%kids%'

OR lower(postdata) LIKE '%krimi%'
OR lower(queryString) LIKE '%krimi%'
OR lower(postdata) LIKE '%crime%'
OR lower(queryString) LIKE '%crime%'

OR lower(postdata) LIKE '%komödie%'
OR lower(queryString) LIKE '%komödie%'
OR lower(postdata) LIKE '%comedy%'
OR lower(queryString) LIKE '%comedy%'

OR lower(postdata) LIKE '%musik%'
OR lower(queryString) LIKE '%musik%'
OR lower(postdata) LIKE '%music%'
OR lower(queryString) LIKE '%music%'

OR lower(postdata) LIKE '%mystery%'
OR lower(queryString) LIKE '%mystery%'

OR lower(postdata) LIKE '%romantik%'
OR lower(queryString) LIKE '%romantik%'
OR lower(postdata) LIKE '%romance%'
OR lower(queryString) LIKE '%romance%'

OR lower(postdata) LIKE '%fiction%'
OR lower(queryString) LIKE '%fiction%'

OR lower(postdata) LIKE '%thriller%'
OR lower(queryString) LIKE '%thriller%'

OR lower(postdata) LIKE '%western%'
OR lower(queryString) LIKE '%western%'
;

