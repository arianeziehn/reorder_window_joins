# Scripts

In this folder, we provide details about our Flink configurations (flink-conf.yaml) and the scripts for our performance evaluation. 
At the top of each script, you find a set of variables (i.e., paths) that need to be adjusted for your cluster.

## Queries
For our queries, we exclusively use the entry classes **IVJClusterT4** for two-way Joins exclusively using Interval Joins, **MixedWindowT4** for two-way Joins using both Sliding Windows and Interval Joins, and **SWJClusterT4** for two-way Joins exclusively using Sliding Window Joins.
All three classes incorporate all proposed features to create equivalent query plans for a two-way window join query
$$[[A x B]^{W_1} x C ]^{W_2}$$ where the order of interest have to be provided, e.g., -order ABC. 

## Maximal Sustainable Throughput
We evaluate the maximal sustainable throughput in preliminary experiments. Note that this is an exploration process, and you have to run each query and permutation separately to evaluate its throughput. 
Depending on your machines, you need to adjust the throughput's (i.e., the ingestion rate) potentially several times. When latency does not constantly increase during execution and 
the ingestion rate is approximately equals the result of the throughput logger the maximal sustainable throughput is identified. Otherwise, these metrics indicate that the sources are throttled due to 
backpressure in the system. 
A maximal sustainable throughput is the maximal throughput the system can reach without creating backpressure on the upstream operators of the execution pipeline.
Thus, leading to a similar value for the ingestion rate and the maximal maintainable throughput. We exploratory identified the maximal maintainable throughput for each query and query using the ThroughputLogger in the util folder.
We ensure that the ingestion rate is equivalent to the derived average throughput (mean(result)) with a tolerance bound of 10%. 
Furthermore, we ensure that the standard deviation of all 10 runs is smaller than 5%. 
You can use our R code below to verify your throughput results, which are printed in the Flink log files. 
```
path <- '/to/your/logFiles.txt'

#result_all = array(dim = c(6,4))
#perm <- c('ABC','ACB', 'BAC', 'BCA','CAB', 'CBA')
#or
result_all = array(dim = c(4,4))
perm <- c('ABC','ACB', 'BAC','CAB')

row.names(result_all) <- perm
for (j in perm){
result=c()
tput = # set ingestion rate
for (i in 1:10){
    data0 <- read.csv(paste(path,paste(j,'_', paste(i,'.txt', sep=""), sep=""), sep=""), header = FALSE, sep ="$")
    dataAGG1 <- aggregate(data0$V9, by = list(data0$V1,data0$V3), FUN = sum, na.rm=TRUE)
    dataAGG2 <- aggregate(dataAGG1$x, by = list(dataAGG1$Group.2), FUN = mean, na.rm=TRUE)
    result[i] <- sum(dataAGG2$x)
}
result_all[j,1] = mean(result) >= tput*0.9 && mean(result) <= tput*1.1
result_all[j,2] = mean(result)
sd(result) <= tput*0.05
result_all[j,3] <- sd(result)
result_all[j,4] <- mean(dataAGG2$x)
}
``` 

## Micro Benchmarks
We use the command line tool Dool [1] to monitor CPU und memory utilization during our performance evaluation. 

[1] https://github.com/scottchiefbaker/dool

