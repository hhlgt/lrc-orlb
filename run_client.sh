# run client
./project/build/run_client true Azur_LRC opt single_rack load_balance 8 4 3 1024 4
# unlimit bandwidth
sh exp.sh 4
# kill datanodes and proxies
sh exp.sh 0
# kill coordinator
pkill -9 run_coordinator
