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
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 10T works as MST for both 30 as MST but takes 3100s that is a bit much, see if time goes down with lower MST 9000 -> 3000s
    $flink run -c SWJCluster $jar --output $output_path --tput 5000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 10T works as MST for 2500s let us check lower
    $flink run -c SWJCluster $jar --output $output_path --tput 6000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
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
  for order in BCA CBA; do
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 3,5T is MST but always over 2000s, we go 500 down
    $flink run -c SWJCluster $jar --output $output_path --tput 3000 --w1size 30 --w1slide 30 --w2slide 30 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 3,5T works but more is possible, 5T is too much, 4T it is MST
    $flink run -c SWJCluster $jar --output $output_path --tput 4000 --w1size 30 --w1slide 45 --w2slide 45 --w2size 30 --run 25 --order $order --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 3 MST
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
    # 3T MST
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
