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
for loop in 1 2 3; do
# Case lb > ub, window size 10
    now=$(date +"%T")
    today=$(date +%d.%m.%y)
    echo "Current time : $today $now" >>$resultFile
    echo "Flink start" >>$resultFile
    $startflink
    START=$(date +%s)
    # 15T works but 2727s
    $flink run -c IVJCluster $jar --output $output_path --tput 12500 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 100 T is MST but 2279
    $flink run -c IVJCluster $jar --output $output_path --tput 90000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 300 okay
    $flink run -c IVJCluster $jar --output $output_path --tput 500 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 70T MST but 2081s
    $flink run -c IVJCluster $jar --output $output_path --tput 60000 --w1lB 10 --w1uB 0 --w2lB 10 --w2uB 0 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 7500 runs bit 4221s - 3800 is from ana
    $flink run -c IVJCluster $jar --output $output_path --tput 3800 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 50 MST
    $flink run -c IVJCluster $jar --output $output_path --tput 50000 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 6 is too much, 3500 is too much (runs but 3940s)- 1200
    $flink run -c IVJCluster $jar --output $output_path --tput 1200 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 500 is too low
    $flink run -c IVJCluster $jar --output $output_path --tput 1320 --w1lB 10 --w1uB 10 --w2lB 10 --w2uB 10 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 4500 runs but 4930s
    $flink run -c IVJCluster $jar --output $output_path --tput 1600 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order ABC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 50T runs bur 2231s
    $flink run -c IVJCluster $jar --output $output_path --tput 45000 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order ACB --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 400 runs try higher (1813s)
    $flink run -c IVJCluster $jar --output $output_path --tput 500 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order BAC --freqA 30 --freqB 15 --para 16 --keys 16
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
    # 400 runs (1757s)
    $flink run -c IVJCluster $jar --output $output_path --tput 600 --w1lB 10 --w1uB 20 --w2lB 10 --w2uB 20 --run 25 --order CAB --freqA 30 --freqB 15 --para 16 --keys 16
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
