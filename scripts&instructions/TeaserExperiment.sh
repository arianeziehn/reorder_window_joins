#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_W/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_W/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_W/bin/flink'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT.jar'
resultFile='/home/ziehn-ldap/CollectTeaserResults.txt'
output_path='/home/ziehn-ldap/result'
data_path1='/home/ziehn-ldap/QnV_R2000070_integrated.csv'
data_path2='/home/ziehn-ldap/luftdaten_11245_integrated.csv'

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1 2 3 4 5 6 7 8 9 10; do
  #SEQ(2) --vel 150 --qua 200 (sel: 3*10^-5)
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayABC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 100000 ## MST 120 is too much, 80 to little
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ABC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  #$flink run -c IntervalJoin3wayACB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 170000 # 160 MST = 165
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayACB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  #$flink run -c IntervalJoin3wayBAC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 115000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayBAC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
 # $flink run -c IntervalJoin3wayCAB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 170000 # 160 MST
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayCAB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayB_AC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 130000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayB_AC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_B_AC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayB_CA $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 130000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayB_CA run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_B_CA_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayC_AB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 130000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayC_AB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_AB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayC_BA $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 130000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayC_BA run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_C_BA_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  #Sliding Window
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayABC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 30000 # 80,60 MST fails
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  #$flink run -c SWJ3wayACB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 160000 ## MST 155
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayACB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayBAC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 30000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayBAC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  #$flink run -c SWJ3wayCAB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000 # MST 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayCAB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayB_AC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 155000 # MST 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayB_AC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_AC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_AC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayB_CA $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 155000 # MST 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayB_CA run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_CA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_B_CA_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayC_AB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 30000 # MST 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayC_AB run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_AB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_AB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayC_BA $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 30000 # MST 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ3wayC_BA run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_BA_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_C_BA_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
done
echo "Tasks executed"
