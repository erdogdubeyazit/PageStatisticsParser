# PageStatisticsParser
## _Pyspark application to parse sample page visit data_

## Input parameters

- pagetype          : Comma seperated 'WEBPAGE_TYPE' values in 'lookup' data.
- metrictype        : Requested statistics. Use 'fre' for time windowed frequencies, 'dur' for recency. Indicators must be comma seperated.
- timewindow        : Comma seperated integers for time windowing.
- dateofreference   : Date string to caculate how many days have passed since the last activity after this date reference.

## Key points

- Small 'lookup' data BROADCAST joined
- Memory partition by :"USER_ID","WEB_PAGEID" and date parts like year, month, day.  
- Data cached before aggregation
- Disk partition by "USER_ID"
- LOG4J for logging
- unitttest for testin

## Running the application
Example usage : 
```bash
spark-submit PageViewStatisticsApplication.py --pagetype 'news, movies' --metrictype 'fre, dur' --timewindow '365, 730, 1460, 2920' --dateofreference '12/10/2019' 
```
