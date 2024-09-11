#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_W/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_W/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_W/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_IVJoins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT_W.jar'
output_path='/local-ssd/ziehn-ldap/result_IVJ'

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1; do
  for order in ABC ACB BAC CAB; do
    for lb in 10 0; do
      for ub in 10 20; do
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c IVJCluster $jar --output $output_path --tput 150 --w1lB $lb --w1uB $ub --w2lB $lb --w2uB $ub --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "IVJ_"$order" run w1("$lb","$ub') w2('$lb','$ub')'$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ'$lb'_'$ub'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ'$lb'_'$ub'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ'$lb'_'$ub'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ'$lb'_'$ub'_'$order'_'$loop'.txt'
  done
  done
done
done
echo "Tasks executed"
