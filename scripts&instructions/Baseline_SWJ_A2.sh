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
for loop in 1 2 3; do
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
      # 10T works as MST for both , 15 T works for both but (4000s for 30 and 3000s for 45), same for 12,5T
      $flink run -c SWJCluster $jar --output $output_path --tput 10000 --w1size 30 --w1slide $slide --w2slide $slide --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
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
    #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3,5T is MST
    $flink run -c SWJCluster $jar --output $output_path --tput 3500 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_30_'$order'_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3,5T works but more is possible
    $flink run -c SWJCluster $jar --output $output_path --tput 5000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FOut_A2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2/FLog_A2_45_'$order'_'$loop'.txt'
  done
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 2T MST
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
    # 2T MST
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
    # 3T works, (3,5 does not work)
    $flink run -c SWJCluster $jar --output $output_path --tput 3000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 3T works, 4T does not
    $flink run -c SWJCluster $jar --output $output_path --tput 3000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
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
