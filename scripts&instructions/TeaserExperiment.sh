#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6/bin/flink'
jar='/home/ziehn-ldap/flink-cep-1.0-SNAPSHOT.jar'
output_path='/home/ziehn-ldap/result'
data_path1='/home/ziehn-ldap/QnV_R2000070.csv'
data_path2='/home/ziehn-ldap/luftdaten_11245.csv'

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
  $flink run -c IntervalJoin3wayABC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJL_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ABC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayACB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_ACB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayBAC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_BAC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c IntervalJoin3wayCAB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_IVJT_CAB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile 
  #Sliding Window
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayABC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ABC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
 now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayACB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_ACB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
 now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayBAC $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_BAC_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile
 now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJ3wayCAB $jar --inputQnV $data_path1 --inputPM $data_path2 --output $output_path --tput 150000
  END=$(date +%s)
  DIFF=$((END - START))
  echo "IntervalJoin3wayABC run "$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.out' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-1-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/flink-taskexecutor-0-node-1.log' '/home/ziehn-ldap/TeaserExp/FOut_SWJT_CAB_'$loop'.txt'
  echo "------------ Flink stopped ------------" >>$resultFile    
done
echo "Tasks executed"
