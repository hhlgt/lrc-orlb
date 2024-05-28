ARRAY=('node2' 'node3' 'node5' 'node6' 'node7' 'node8' 'node9' 'node10' 'node11' 'node12' 'node18')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`
SRC_PATH1=/home/GuanTian/lrc-orlb/run_cluster_sh/
SRC_PATH2=/home/GuanTian/lrc-orlb/project
SRC_PATH3=/home/GuanTian/wondershaper
SRC_PATH4=/home/GuanTian/lrc-orlb/kill_proxy_datanode_redis.sh

DIS_DIR1=/home/GuanTian/lrc-orlb
DIS_DIR2=/home/GuanTian/wondershaper

# if simulate cross-cluster transfer
if [ $1 == 5 ]; then
    ssh GuanTian@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000 -u 1000000'
elif [ $1 == 6 ]; then
    ssh GuanTian@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
else
    echo "cluster_number:"${#ARRAY[@]}
for i in $(seq 0 $NUM)
    do
    temp=${ARRAY[$i]}
        echo $temp
        if [ $1 == 0 ]; then
            if [ $temp == 'node18' ]; then
                ssh GuanTian@$temp 'pkill -9 run_datanode;pkill -9 redis-server'
            else
                ssh GuanTian@$temp 'pkill -9 run_datanode;pkill -9 run_proxy;pkill -9 redis-server'
            fi
            echo 'pkill  all'
            ssh GuanTian@$temp 'ps -aux | grep redis-server | wc -l'
            ssh GuanTian@$temp 'ps -aux | grep run_datanode | wc -l'
            ssh GuanTian@$temp 'ps -aux | grep run_proxy | wc -l'
        elif [ $1 == 1 ]; then
            ssh GuanTian@$temp 'cd /home/GuanTian/lrc-orlb;bash cluster_run_redis.sh;bash cluster_run_proxy_datanode.sh'
            echo 'proxy_datanode process number:'
            ssh GuanTian@$temp 'ps -aux |grep redis-server | wc -l;ps -aux |grep run_datanode | wc -l;ps -aux |grep run_proxy | wc -l'
        elif [ $1 == 2 ]; then
            ssh GuanTian@$temp 'mkdir -p' ${DIS_DIR1}
            ssh GuanTian@$temp 'mkdir -p' ${DIS_DIR2}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_redis.sh GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy_datanode.sh GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH2} GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH3} GuanTian@$temp:${DIS_DIR2}
            rsync -rtvpl ${SRC_PATH4} GuanTian@$temp:${DIS_DIR1}
        elif [ $1 == 3 ]; then   # if not simulate cross-cluster transfer
            ssh GuanTian@$temp 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000 -u 1000000'
        elif [ $1 == 4 ]; then
            ssh GuanTian@$temp 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
    done
fi