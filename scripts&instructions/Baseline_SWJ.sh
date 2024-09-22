#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_Joins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT.jar'
output_path='/local-ssd/ziehn-ldap/result_SWJ'

# freq is tuples per hour, highlevel in 60 time units while window sizes are in time units
# that means freq 30 creates a new tuple every 2 minutes

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1 2 3 4; do
      # A4 - ABC
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 8,5T MST
      $flink run -c SWJCluster $jar --output $output_path --tput 8500 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ABC run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_ABC_'$loop'.txt'
      # A4
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 15 is MST
      $flink run -c SWJCluster $jar --output $output_path --tput 17500 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ABC run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_ABC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_ABC_'$loop'.txt'
      # A4 - ACB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #25T is MST but time is 2300s (30 is too much) try if better with 20
      $flink run -c SWJCluster $jar --output $output_path --tput 18000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ACB run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_ACB_'$loop'.txt'
      # A4
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 35T is MST
      $flink run -c SWJCluster $jar --output $output_path --tput 37500 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ACB run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_ACB_'$loop'.txt'
     # A4 - BCA
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 12500 MST try like CBA
      $flink run -c SWJCluster $jar --output $output_path --tput 16000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order BCA --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BCA run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_BCA_'$loop'.txt'
      # A4
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #30MST
      $flink run -c SWJCluster $jar --output $output_path --tput 30000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order BCA --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BCA run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_BCA_'$loop'.txt'
     # A4 - CAB
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #MST 25 works but 2400s test with
      $flink run -c SWJCluster $jar --output $output_path --tput 20000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CAB run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_CAB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 30T 1855s 20T 1794s test with same as ACB (35T)
      $flink run -c SWJCluster $jar --output $output_path --tput 35000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CAB run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_CAB_'$loop'.txt'
     # A4 - BAC
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #MST 8,5 is MST
      $flink run -c SWJCluster $jar --output $output_path --tput 8500 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BAC run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_BAC_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 15T is MST, 17,5 fails
      $flink run -c SWJCluster $jar --output $output_path --tput 15000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BAC run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_BAC_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_BAC_'$loop'.txt'
    # A4 - CBA
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #MST 12,5 is MST lets try 16 is 50% more than ABC
      $flink run -c SWJCluster $jar --output $output_path --tput 16000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order CBA --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CBA run w1(30,30) w2(5,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_30_CBA_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 20T is MST, test 30T as BCA
      $flink run -c SWJCluster $jar --output $output_path --tput 30000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order CBA --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CBA run w1(30,45) w2(5,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_45_CBA_'$loop'.txt'
for order in ABC BAC; do
    # TW cases
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 35T no, 30 test (27,5 is MST)
      $flink run -c SWJCluster $jar --output $output_path --tput 35000 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_"$order" run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 35T yes timeing 1600s, 40 is 1665s -> 375 MTS
      $flink run -c SWJCluster $jar --output $output_path --tput 60000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_"$order" run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
 done
  for order in ACB BCA CAB CBA; do
    # TW cases
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # higher from 25T to 35T (15T all made) 35T is oksy but 1800s, 32500 still 1780s (30T is MST for all works)
      $flink run -c SWJCluster $jar --output $output_path --tput 60000 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_"$order" run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_'$order'_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #  40T MST with 1660s
      $flink run -c SWJCluster $jar --output $output_path --tput 80000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_"$order" run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_'$order'_'$loop'.txt'
done
done
echo "Tasks executed"
