# limit bandwidth
sh exp.sh 3
# run datanodes and proxies
sh exp.sh 1
# run coordinator
./project/cmake/build/run_coordinator > xxx.txt &