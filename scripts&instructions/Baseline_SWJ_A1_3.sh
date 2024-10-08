#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_Joins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT_SWJ.jar'
output_path='/local-ssd/ziehn-ldap/result_SWJ_A1_3'

# freq is tuples per 'hour' (event time), highlevel in 60 time units while window sizes are in time units
# that means freq 30 creates a new tuple every 2 minutes

filter=false
now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
#new Q3
for loop in 1 2 3 4 5; do
     # 30T no all, 15T okay only CAB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #15 works is MST
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 14000 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order CAB --freqA 30 --freqB 15 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A3 run w1(10,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_CAB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 10 T fails, 8T is too high, 6T is fine
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 7000 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order BAC --freqA 30 --freqB 15 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_BAC_A3 run w1(10,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_BAC_'$loop'.txt'
 # CAB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #12,5 works, 15 works
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 15000 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ACB --freqA 30 --freqB 15 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_ACB_A3 run w1(10,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_ACB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 9000 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ABC --freqA 30 --freqB 15 --freqC 1 --para 16 --keys 16 --filter $filter
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_ABC_A3 run w1(10,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S1_s2_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S1_s2_ABC_'$loop'.txt'
done
#Setting 1 with same rates
for loop in 1 2 3 4 5; do
     # CAB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 2600 --w1size 20 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A1 run w1(20,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_CAB_'$loop'.txt'
      # A3 slide < length
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #3000 is too high
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 2800 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A3 run w1(20,2) w2(10,2)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_CAB_'$loop'.txt'
      # A1 slide = 10
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #8,5 works fine 10,5T works but variance is very high
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 9000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_CAB_'$loop'.txt'
      # A3 slide < length
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #14 works fine 685468.9  6931.786 42546.60 
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 17000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order CAB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_CAB_'$loop'.txt'
#BAC
    # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #700 works but is tooo high 500 works (also MST)
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 600 --w1size 20 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC run w1(20,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_BAC_'$loop'.txt'
    # A3 slide < length
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 6000 fails 4500 fails 2000 works but too high, 1750 works fine
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 1850 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A3 run w1(20,2) w2(10,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_BAC_'$loop'.txt'
    # A1 slide = 10
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #15 too high variance
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 9000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_BAC_'$loop'.txt'
    # A3 slide < length
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #20T fails, 15 T looks good (auch MST)
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 17500 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_BAC_'$loop'.txt'
# ACB
   # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2,5 fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 2250 --w1size 20 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A1 run w1(20,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #10500 looks fine 15 T fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 9000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 4T fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 3250 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB run w1(20,2) w2(10,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #13 look good 18 T fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 17000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order ACB --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_ACB_'$loop'.txt'
# ABC
   # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #1050 works but high time, 900 same
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 650 --w1size 20 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC run w1(20,2) w2(20,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s2_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #15T fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 9000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A1S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A1S2_s10_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2050 works but high 1550 tiny problems
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 1450 --w1size 10 --w1slide 2 --w2slide 2 --w2size 20 --run 25 --order ABC --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A3 run w1(20,2) w2(10,2)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s2_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #25 T fails 15T works fine
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 17000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FOut_A3S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3_S2/FLog_A3S2_s10_ABC_'$loop'.txt'
done
echo "Tasks executed"
