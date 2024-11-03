# Util Folder

## UDF
basic and repetitively used classes for queries, i.e., KeySelector or TimeStampAssigners. 

## SourceFunction 

The **ArtificialSourceFunction** is a RichParallelSource function that is executed in parallel
and creates the multiple keys, i.e., number of keys can be specified but have to be larger than max parallelism of environment.  
The output is a Tuple3<Key(Int),Value(Int),EventTime(Long)> (value is a random Int). 

**ArtificialSourceFunctionT4** is equivalent to **ArtificialSourceFunction** but adds a system time timestamp for latency evaluation

**Tuple3ParallelSourceFunction** is used for Case Simulation, i.e., it reads data from a csv source to enable equivalence evaluation of output. 

## Metrics

We use the **ThroughputLogger** and **LatencyLoggerT10** classes to monitor the throughput of our application for a 2-way Window Join. Be aware that placing
the logger needs to be done carefully as it may cause additional shuffling processes otherwise. 
