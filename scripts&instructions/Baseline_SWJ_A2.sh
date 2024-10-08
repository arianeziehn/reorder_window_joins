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
for loop in 1 2 3 4 5; do
# same rates
  for order in ACB; do
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 50500 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #23 works 1148385.2 15548.72053  71963.53
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 75000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
  done
    for order in CAB; do
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 75 fails
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 60000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_A2"$order" run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
      now=$(date +"%T")
      today=$(date +%d.%m.%y)
      echo "Current time : $today $now" >>$resultFile
      echo "Flink start" >>$resultFile
      $startflink
      START=$(date +%s)
      # 75 runs
      $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 100000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
      END=$(date +%s)
      DIFF=$((END - START))
      echo "SWJ_A2"$order" run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
      $stopflink
      echo "------------ Flink stopped ------------" >>$resultFile
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
      cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
    done
  for order in BCA CBA; do
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #75 fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 55000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_'$order'_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #75 works
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 90000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2"$order" run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_'$order'_'$loop'.txt'
  done
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #75 fails
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 65000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 ABC run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_ABC_'$loop'.txt'
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #6T works 288085.2     76.61241 18000.00
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 65000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 BAC run w1(30,30) w2(30,30)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_30_BAC_'$loop'.txt'
    #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    #8,5T works 408050.3    59.72899  25502.05
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 85050 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order ABC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 ABC run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_ABC_'$loop'.txt'
   #A2
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 8,5 T works 408176.0   220.90014  25504.63
    $flink run -c SWJClusterT4_2 $jar --output $output_path --tput 85000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order BAC --freqA 15 --freqB 15 --freqC 15 --para 16 --keys 16 --filter true
    END=$(date +%s)
    DIFF=$((END - START))
    echo "SWJ_A2 BAC run w1(30,45) w2(30,45)"$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.out' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FOut_A2S2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_A2/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-25.log' '/home/ziehn-ldap/BaselineExp/result_SWJ_A2_S2/FLog_A2S2_45_BAC_'$loop'.txt'
done
echo "Tasks executed"
