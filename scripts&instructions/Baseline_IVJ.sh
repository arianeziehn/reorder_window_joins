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
for loop in 1 2 3 4 5 6 7 8 9 10; do
# Case lb > ub, window size 10
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 20 is too much
    $flink run -c IVJCluster $jar --output $output_path --tput 9500 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 100 T is MST but 2279, 90 T is 2199 but MST 110, same time does not reduce also not for 80T, try to find fail; 110T -> 2.640 expected, makes 3.176; try 120T
    # 120T suggest 140T as throughput, lets go, else 120T are MST
    $flink run -c IVJCluster $jar --output $output_path --tput 140000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 1000 okay, 10T is too much, 8T does not hold for MST, suggest 6,9T
    $flink run -c IVJCluster $jar --output $output_path --tput 6900 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 70T MST but 2081s 60T is 2233 s so better 70T, 110*1,5 = 165 -> MST 2.640 we get 2.894, 135 also hold for MST
    $flink run -c IVJCluster $jar --output $output_path --tput 160300 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,0) w2(10,0) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_0_CAB_'$loop'.txt'
# Case lb = ub, window size 20
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 900 is not MST, lower, 800 is too low
    $flink run -c IVJCluster $jar --output $output_path --tput 850 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 35 too high, through 35 instead of 35*1,5, 25 is too low
    $flink run -c IVJCluster $jar --output $output_path --tput 35000 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 950 is MST
    $flink run -c IVJCluster $jar --output $output_path --tput 950 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 16500 yields 820246 MST, try 35T (from ana), 35T is too high
    $flink run -c IVJCluster $jar --output $output_path --tput 27500 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,10) w2(10,10) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_10_CAB_'$loop'.txt'
# Case lb < ub, window size 30
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 800 is MST
    $flink run -c IVJCluster $jar --output $output_path --tput 800 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ABC run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ABC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ABC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 50T runs not 100% MST a bit higher (throughput and sd) to let us give some more 55T
    $flink run -c IVJCluster $jar --output $output_path --tput 62000 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_ACB run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ACB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_ACB_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 500 runs try higher (1813s), analysis request 1000 (time 750 - 2190s), 1000 is too much
    $flink run -c IVJCluster $jar --output $output_path --tput 850 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_BAC run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_BAC_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_BAC_'$loop'.txt'
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 50 is MST
    $flink run -c IVJCluster $jar --output $output_path --tput 50000 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
    END=$(date +%s)
    DIFF=$((END - START))
    echo "IVJ_CAB run w1(10,20) w2(10,20) "$loop " : "$DIFF"s" >>$resultFile
    $stopflink
    echo "------------ Flink stopped ------------" >>$resultFile
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.out' '/home/ziehn-ldap/BaselineExp/result_IVJ/FOut_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-1-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_CAB_'$loop'.txt'
    cp '/home/ziehn-ldap/flink-1.11.6_W/log/''flink-ziehn-ldap -taskexecutor-0-sr630-wn-a-48.log' '/home/ziehn-ldap/BaselineExp/result_IVJ/FLog_IVJ_10_20_CAB_'$loop'.txt'
done
echo "Tasks executed"
