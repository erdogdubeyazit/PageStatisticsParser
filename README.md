# PageStatisticsParser
## _Pyspark application to parse sample page visit data_

## Key points

- Small 'lookup' data BROADCAST joined
- Memory partition by :"USER_ID","WEB_PAGEID" and date parts like year, month, day.  
- Disk partition by "USER_ID"
- LOG4J for logging
- unitttest for testin

## Running the application
Example usage : 
```bash
spark-submit PageViewStatisticsApplication.py --pagetype 'news, movies' --metrictype 'fre, dur' --timewindow '365, 730, 1460, 2920' --dateofreference '12/10/2019' 
```
