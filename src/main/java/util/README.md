# Util Folder

## UDF
basic and repetitively used classes for the patterns and queries, i.e., KeySelector or TimeStampAssigners. 
for the different queries. 

## SourceFunction 

The **ArtificialSourceFunction** is a Richparallelsource function that is executed in parallel
and creates the multiple keys.  

## Metrics

We use the **ThroughputLogger** and **LatencyLoggerT9** classes to monitor the throughput of our application. Be aware that placing
the logger needs to be done carefully as it may cause additional shuffling processes otherwise. 
