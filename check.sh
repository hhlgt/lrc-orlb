ARRAY=('node2' 'node3' 'node5' 'node6' 'node7' 'node8' 'node9' 'node10' 'node11' 'node12' 'node13' 
       'node14' 'node15' 'node16' 'node17' 'node18')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`

for i in $(seq 0 $NUM)
do
temp=${ARRAY[$i]}
    echo $temp
    ssh GuanTian@$temp 'ps -aux | grep redis-server | wc -l'
    ssh GuanTian@$temp 'ps -aux | grep run_datanode | wc -l'
    ssh GuanTian@$temp 'ps -aux | grep run_proxy | wc -l'
done