#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_SWJ/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_Joins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT_SWJ.jar'
output_path='/local-ssd/ziehn-ldap/result_SWJ_A1_3'

# freq is tuples per hour, highlevel in 60 time units while window sizes are in time units
# that means freq 30 creates a new tuple every 2 minutes

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1 2 3; do
     # CAB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # works for 400 but it takes really lon, reduce a tiny bit (11944s) 200 (6340)
      $flink run -c SWJCluster $jar --output $output_path --tput 100 --w1size 20 --w1slide 1 --w2slide 1 --w2size 20 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A1 run w1(20,1) w2(20,1)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_CAB_'$loop'.txt'
      # A3 slide < length
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 1935s but 500 MST worked 400 4131s 300 (3548)
      $flink run -c SWJCluster $jar --output $output_path --tput 200 --w1size 20 --w1slide 1 --w2slide 1 --w2size 10 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A3 run w1(20,1) w2(10,1)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_CAB_'$loop'.txt'
      # A1 slide = 10
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 2000 is too much, 1000 works, we try 1500, 900 also works (1751s), 850 (1736)
      $flink run -c SWJCluster $jar --output $output_path --tput 1000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_CAB_'$loop'.txt'
      # A3 slide < length
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 1600 MST
      $flink run -c SWJCluster $jar --output $output_path --tput 1600 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_CAB_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_CAB_'$loop'.txt'
#BAC
    # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    $flink run -c SWJCluster $jar --output $output_path --tput 50 --w1size 20 --w1slide 1 --w2slide 1 --w2size 20 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC run w1(20,1) w2(20,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_BAC_'$loop'.txt'
    # A3 slide < length
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    $flink run -c SWJCluster $jar --output $output_path --tput 75 --w1size 20 --w1slide 1 --w2slide 1 --w2size 10 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A3 run w1(20,1) w2(10,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_BAC_'$loop'.txt'
    # A1 slide = 10
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 850 MST
    $flink run -c SWJCluster $jar --output $output_path --tput 850 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_BAC_'$loop'.txt'
    # A3 slide < length
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 1000 is MST
    $flink run -c SWJCluster $jar --output $output_path --tput 1000 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_BAC_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_BAC_'$loop'.txt'
# ACB
   # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # works but also takes long 14000s (as CAB), 400 11944, 200 is 6305s
    $flink run -c SWJCluster $jar --output $output_path --tput 100 --w1size 20 --w1slide 1 --w2slide 1 --w2size 20 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A1 run w1(20,1) w2(20,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # MST 1300
    $flink run -c SWJCluster $jar --output $output_path --tput 1300 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 1000 seems to but takes very very look 90 min, with 800 down to 6645 s reduce further, 600 with 5000s
    $flink run -c SWJCluster $jar --output $output_path --tput 200 --w1size 20 --w1slide 1 --w2slide 1 --w2size 10 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB run w1(20,1) w2(10,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 1350 MST
    $flink run -c SWJCluster $jar --output $output_path --tput 1350 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ACB_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_ACB_'$loop'.txt'
# ABC
   # A1 slide = 1
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 500 too much
    $flink run -c SWJCluster $jar --output $output_path --tput 50 --w1size 20 --w1slide 1 --w2slide 1 --w2size 20 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC run w1(20,1) w2(20,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s1_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2000 is too much, 1000 is okay, let us try 1500, 900 1749s, 800 2160s
    $flink run -c SWJCluster $jar --output $output_path --tput 750 --w1size 20 --w1slide 10 --w2slide 10 --w2size 20 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A1 run w1(20,10) w2(20,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A1_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A1_s10_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # MST 100
    $flink run -c SWJCluster $jar --output $output_path --tput 100 --w1size 20 --w1slide 1 --w2slide 1 --w2size 10 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A3 run w1(20,1) w2(10,1)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s1_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # MST 950
    $flink run -c SWJCluster $jar --output $output_path --tput 950 --w1size 20 --w1slide 10 --w2slide 10 --w2size 15 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_ABC_A3 run w1(20,10) w2(15,10)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FOut_A3_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_SWJ/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-13.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A1_3/FLog_A3_s10_ABC_'$loop'.txt'
done
echo "Tasks executed"
