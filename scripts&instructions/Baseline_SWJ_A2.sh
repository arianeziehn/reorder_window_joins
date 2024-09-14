#!/usr/bin/env bash
startflink='/home/ziehn-ldap/flink-1.11.6_A2/bin/start-cluster.sh'
stopflink='/home/ziehn-ldap/flink-1.11.6_A2/bin/stop-cluster.sh'
flink='/home/ziehn-ldap/flink-1.11.6_A2/bin/flink'
resultFile='/local-ssd/ziehn-ldap/BaselineExp_Joins.txt'
jar='/home/ziehn-ldap/flink-joinOrder-1.0-SNAPSHOT_A2.jar'
output_path='/local-ssd/ziehn-ldap/result_SWJ_A2'

# freq is tuples per hour, highlevel in 60 time units while window sizes are in time units
# that means freq 30 creates a new tuple every 2 minutes

now=$(date +"%T")
today=$(date +%d.%m.%y)
echo "Current time : $today $now" >>$resultFile
echo "----------$today $now------------" >>$resultFile
for loop in 1; do
  for order in ACB CAB; do
    # TW cases
      for slide in 30 45; do
      #A2
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 10T works as MST for both , 20T does not work for 30, for 45 it works but takes 3594s we try lower
      $flink run -c SWJCluster $jar --output $output_path --tput 15000 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_A2"$order" run w1(30,"$slide") w2(30,"$slide")"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_'$slide'_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_'$slide'_'$order'_'$loop'.txt'
      done
    done
  for order in BCA CBA; do
  # TW cases
    for slide in 30 45; do
    #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3T works let go higher,5T is too much (2300s for 30, 2000 for 45)
    $flink run -c SWJCluster $jar --output $output_path --tput 4000 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,"$slide") w2(30,"$slide")"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_'$slide'_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_'$slide'_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_'$slide'_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_'$slide'_'$order'_'$loop'.txt'
    done
  done
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2.5T fails
    $flink run -c SWJCluster $jar --output $output_path --tput 2000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 ABC run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_ABC_'$loop'.txt'
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2.5T fails
    $flink run -c SWJCluster $jar --output $output_path --tput 2000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 BAC run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_BAC_'$loop'.txt'
    #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3T works, 5T does not
    $flink run -c SWJCluster $jar --output $output_path --tput 4000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 ABC run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_ABC_'$loop'.txt'
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3T works, 5T does not
    $flink run -c SWJCluster $jar --output $output_path --tput 4000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 BAC run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_BAC_'$loop'.txt'
done
echo "Tasks executed"
