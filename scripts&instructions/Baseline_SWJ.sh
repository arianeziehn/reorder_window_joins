#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_Joins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT.jar'
output_path='/local-ssd/ziehn-ldap/result_SWJ'

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1; do
  for order in ABC ACB BCA BAC CAB CBA; do
   # A4
    for slide in 30 45; do
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJCluster $jar --output $output_path --tput 75 --w1size 5 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_"$order" run w1(5,"$slide') w2(30,'$slide')'$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w2_gth_w1_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w2_gth_w1_'$slide'_'$order'_'$loop'.txt'
  # A4
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJCluster $jar --output $output_path --tput 75 --w1size 30 --w1slide $slide --w2slide $slide --w2size 5 --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      # shellcheck disable=SC2027
      echo "SWJ_"$order" run w1(5,"$slide') w2(30,'$slide')'$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A4_w1_gth_w2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A4_w1_gth_w2_'$slide'_'$order'_'$loop'.txt'
     #A2
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      $flink run -c SWJCluster $jar --output $output_path --tput 75 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_"$order" run w1(5,"$slide") w2(30,"$slide")"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A2_'$slide'_'$order'_'$loop'.txt'
    done
  for slide in 1 15; do
 # A1 slide < length
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJCluster $jar --output $output_path --tput 40 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ_"$order" run w1(5,"$slide") w2(30,"$slide")"$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  echo "------------ Flink stopped ------------" >>$resultFile
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A1_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A1_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A1_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A1_'$slide'_'$order'_'$loop'.txt'
  # A3 slide < length
  now=$(date +"%T")
  today=$(date +%d.%m.%y)
  echo "Current time : $today $now" >>$resultFile
  echo "Flink start" >>$resultFile
  $startflink
  START=$(date +%s)
  $flink run -c SWJCluster $jar --output $output_path --tput 40 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 4 --freqB 2 --para 2 --keys 16
  END=$(date +%s)
  DIFF=$((END - START))
  echo "SWJ_"$order" run w1(5,"$slide") w2(30,"$slide")"$loop " : "$DIFF"s" >>$resultFile
  $stopflink
  echo "------------ Flink stopped ------------" >>$resultFile
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A3_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.out' '/home/ziehn-ldap/BaselineExp/result_SWJ/FOut_A3_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A3_'$slide'_'$order'_'$loop'.txt'
  cp '/home/ziehn-ldap/flink-1.11.6/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-21.log' '/home/ziehn-ldap/BaselineExp/result_SWJ/FLog_A3_'$slide'_'$order'_'$loop'.txt'
  done
done
done
echo "Tasks executed"
