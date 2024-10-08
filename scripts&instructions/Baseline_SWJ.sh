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
for loop in 1 2 3 4 5; do
      # A4 - ABC
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 80T above fails 60 works: 2921716  39161.206 185429.2 not higher than 60!
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 55000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
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
      #115T fails, 85 works 4114113   11979.163 256357.7 not higher
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 80000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order ABC --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 150T fails, 100 works 4838265   2254.735 302456.3
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 125000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order ACB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 190T fails 180 works : 8739090   21228.114 546263.8
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 185000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order ACB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      #115 fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 100000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order BCA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      #150T fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 130000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order BCA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      #120 fails, 90 works 4362436  13785.231 273646.7
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 110000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order CAB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 185 works 8954565  7374.822 559334.4, 200 FAILS
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 190000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order CAB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      #43500 works 2113824  16203.635 132140.1 not higher!
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 40500 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order BAC --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 57500 works 80500 does not work
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 65500 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order BAC --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      #70T fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 60000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 5 --run 25 --order CBA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 85T fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 75000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 5 --run 25 --order CBA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
for order in ABC BAC; do # BAC missing
    # TW cases
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #110 T works 5319793  2949.162 332356.8 5334060 12993.084 333953.0 170 FAILS
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 140000 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
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
      # 250 FAILS
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 200000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
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
    # TW cases
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 125 fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 100000 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order CBA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CBA run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_CBA_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 150000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order CBA --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CBA run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_CBA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_CBA_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 65 too much
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 60000 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order CAB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CAB run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_CAB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 310T fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 290000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_CAB run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_CAB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_CAB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 77 fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 47500 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order ACB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ACB run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_ACB_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 125 is too much
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 100000 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order ACB --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_ACB run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_ACB_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_ACB_'$loop'.txt'
    # TW cases
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      #138 no
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 118500 --w1size 5 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order BCA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BCA run w1(5,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_30_BCA_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 152 no
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 122500 --w1size 5 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order BCA --freqA 15 --freqB 15  --freqC 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_A4_BCA run w1(5,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_BCA_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_45_BCA_'$loop'.txt'
done
echo "Tasks executed"
