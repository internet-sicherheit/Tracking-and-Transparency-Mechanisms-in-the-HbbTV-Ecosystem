# 02_Measurement_Data
(Download the data file from [Zenodo](10.5281/zenodo.15062968))
The files in this folder contains the raw measurement data. 
Inside the ZIP file are the results for each measurement run (total 5).

The data is stored in [HAR-files](https://w3c.github.io/web-performance/specs/HAR/Overview.html#sec-object-types-log). To insert the data from HAR-file
to BigQuery database, we provide a [script](/code/).

## Structure
After unzipping, the following structure appears:
```
Measurements
│   channelsid_per_profile_with_http_traffic.txt  
│
└───2023-10-31 - Profile 1
│   │   channellist.txt
│   │
│   └───hardump
│   │   │   1_2023-10.31_13-44-39_dump.har
│   │   │   1_2023-10.31_13-52-41_dump.har
│   │   │   ...
│   │
│   └───meta_data
│   │   │   Metadata_2023-10-31_13-36-12.txt
│   │   │   Metadata_2023-11-06_11-05-45.txt
│   │
│   └───tv_export
│       │   1_cookies_1699258958_test.tar
│       │   1_cookies_1699367181_test.tar
│       │   1_local_storage_1699258958_test.tar
│       │   1_local_storage_1699367181_test.tar
│   
└───2023-09-14 - Profile 3
│   │   channellist.txt
│   │  ...
```