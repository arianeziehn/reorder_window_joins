#!/usr/bin/env bash
startflink='/pathTo/flink-1.11.6_A2/bin/start-cluster.sh'
stopflink='/pathTo/flink-1.11.6_A2/bin/stop-cluster.sh'
flink='/pathTo/flink-1.11.6_A2/bin/flink'
resultFile='/pathTo/BaselineExp_Joins.txt'
jar='/pathTo/flink-joinOrder-1.0-SNAPSHOT_A2.jar'
output_path='/pathTo/result_mixedWindow'

# freq is tuples per 'hour' (event time), highlevel in 60 time units while window sizes are in time units
# that means freq 30 creates a new tuple every 2 minutes

filter=false
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
#Setting 1 with same rates
for loop in 1 2 3 4 5 6 7 8 9 10; do
     # SWJ order can be CAB, ABC, BAC, BCA, CBA, ACB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJClusterT4 $jar --output $output_path --tput 400000 --w1size 60 --w1slide 30 --w2slide 5 --w2size 5 --run 25 --order CAB --freqA 15 --freqB 100 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB run SWJ(60,30) SWJ(5,5)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      sleep 5m 
      # recover time of flink to free up memory
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.out' '/pathTo/BaselineExp/result_SWJ/FOut_CAB_'$loop'.txt'
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.log' '/pathTo/BaselineExp/result_SWJ/FLog_CAB_'$loop'.txt'
      # Mixed Window: SWJ = W1 and IVJ = W2, order can be CAB, ABC, BAC, ACB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c MixedWindowT4 $jar --output $output_path --tput 17000 --w1size 30 --w1slide 15 --w2lb 0 --w2ub 10 --run 25 --order CAB --freqA 15 --freqB 100 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "Mixed_CAB run SWJ(30,15) w2(0,10)"$loop " : "$DIFF"s" >>$resultFile
      sleep 5m
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.out' '/pathTo/BaselineExp/result_MixedWindow/FOut_CAB_'$loop'.txt'
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.log' '/pathTo/BaselineExp/result_MixedWindow/FLog_CAB_'$loop'.txt'
      # IVJ, order can be CAB, ABC, BAC, ACB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c IVJClusterT4 $jar --output $output_path --tput 9500 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 0 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "IVJ_CAB_A1 run w1(10,10) w2(10,0)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.out' '/pathTo/BaselineExp/result_IVJ/FOut_CAB_'$loop'.txt'
      cp '/pathTo/flink-1.11.6_A2/log/taskexecutor-0-sr630-wn-a-25.log' '/pathTo/BaselineExp/result_IVJ/FLog_CAB_'$loop'.txt'
done
echo "Tasks executed"
