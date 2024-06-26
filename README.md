## Prototype

The architecture follows master-worker style, like many state-of-art distributed file storage such as HDFS and Ceph. Four major components are client, coordinator, proxy and datanode. 

## Quick Start

### Environment Configuration

- Required `gcc` version

  - At least `gcc 11.1.0`

- Required packages

  * yalantinglibs

  * gf-complete & jerasure

  * hredis & redis-plus-plus

  * redis

- we call them third_party libraries, run the following command to install these packages

  ```shell
  sh install_third_party.sh
  ```

### Compile and Run

- Compile

```shell
cd project
sh compile.sh
```

- Run

  - run in a local node for test

    ```shell
    sh run_redis.sh
    sh run_proxy_datanode.sh
    cd project/build
    ./run_coordinator
    ./run_client ...
    # to kill process
    sh kill_proxy_datanode_redis.sh
    ```

  - run in multiple nodes for experiments

    - first run `exp.sh` with argurement `2` to move necessary files to other nodes
    - then

    ```shell
    sh run_server.sh	// run redis-server, datanode, proxy in order
    sh run_client.sh
    ```


- The parameter meaning of `run_client`

  ```shell
  ./run_client partial_decoding encode_type partitioning_type fault_tolerance_level node_selection_type k l g block_size(KB) stripe_num logfile
  ```

  - `partial_decoding` if adopt encode-and-transfer during repair process
  - `encode_type` erasure coding type, now supports `Azu_LRC`(Microsoft's Azure LRC)
  - `partitioning_type` data placement scheme of a single stripe
    - `flat` a block in a unique partition
    - `ran` random number of blocks in a unique partition
    - `ecwide` for `Par-1`, only for `single-rack` fault tolerance
    - `opt` for `Par-2`, our proposed placement strategy
  - `fault_tolerance_level` the level of fault_tolerance_level
    - for system with two hierarchy `rack-node`
      - `single-rack`, can combined with `ran`, `opt`
      - `two-racks`, can combined with `ran`, `opt` 
      - `three-racks`, can combined with `ran`, `opt`
      - `four-racks`, can combined with `ran`, `opt`
      - `random`, can combined with `ran`
  - `node_selection_type`
    - `random`, randomly select nodes for each partition
    - `load_balance`, select nodes for each partition based on load balance
  - `k,l,g` the coding parameters of `LRC` stripes
  - `block_size` denotes the size of each block in a stripe, with the unit of `KiB`
  - `stripe_num` denotes the number of stripes in the storage system for test
  - `logfile` (optional) denotes the information output file in new dir `res` when setting `LOF_IN_FILE` as `true` in `utils.h`

### Others

- Directory `project/` is the system implementation.

- The datanodes store data in memory using `redis` in default. If based on disk, define `IN_MEMORY` as `false` in `utils.h`, and create directory `storage/` to store the data blocks for data nodes.

- Create directory `run_cluster_sh/` to store the running shell for each cluster, each cluster with a two file in a sub-directory. Then, use `tools/generator_sh.py` to generate configuration file and running shell for proxy and data node.

- obtaining `wondershaper` for bandwidth constraint

  ```shell
  git clone https://github.com/magnific0/wondershaper.git
  ```

------

### Some Helpful Tips

#### Install `gcc-11`&`g++-11` in global environment on `Ubuntu 20.04`

1. add the install source

   ```shell
   sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
   ```

2. install `gcc-11`&`g++-11`

   ```shell
   sudo apt-get install gcc-11 g++-11
   ```

3. verify

   ```shell
   gcc-11 -v
   g++-11 -v
   ```

4. set the priority of different `gcc` versions, the one with the highest score is regarded as the default one, for example

   ```
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 --slave /usr/bin/g++ g++ /usr/bin/g++-11 --slave /usr/bin/gcov gcov /usr/bin/gcov-11
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 95 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9 --slave /usr/bin/gcov gcov /usr/bin/gcov-9
   ```

5. modify the default version, run the command and choose the version

   ```
   sudo update-alternatives --config gcc
   ```

#### Install `gcc-11`&`g++-11` in user's local environment

- a shell script

```shell
PACKAGE_DIR=/path/to/where/tar_package/in
INSTALL_DIR=/path/to/install
GCC_DIR=$PACKAGE_DIR/gcc-$GCC_VERSION
GCC_INSTALL_DIR=$INSTALL_DIR/gcc-$GCC_VERSION

mkdir -p $GCC_INSTALL_DIR
cd $GCC_INSTALL_DIR
rm -rf *
cd $PACKAGE_DIR
rm -rf gcc-$GCC_VERSION
if [ ! -f "gcc-${GCC_VERSION}.tar.gz" ]; then
	wget --no-check-certificate https://gcc.gnu.org/pub/gcc/releases/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.gz
fi
tar -xvzf gcc-${GCC_VERSION}.tar.gz
cd $GCC_DIR
./contrib/download_prerequisites
./configure --prefix=$GCC_INSTALL_DIR --disable-multilib --enable-checking=release 
make -j6
make install
```

- set up the environment

```shell
TEMPORARY_SETTING=0

if [ ${TEMPORARY_SETTING} -eq 1 ]; then
	export PATH=${GCC_INSTALL_DIR}/bin:\$PATH
	export CC=${GCC_INSTALL_DIR}/bin/gcc
	export CXX=${GCC_INSTALL_DIR}/bin/g++
	export LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:$LIBRARY_PATH
	export LD_LIBRARY_PATH=${GCC_INSTALL_DIR}/lib64:$LD_LIBRARY_PATH
else
	sudo echo "" >> ~/.bashrc
	sudo echo "export PATH=${GCC_INSTALL_DIR}/bin:\$PATH" >> ~/.bashrc
	sudo echo "export CC=${GCC_INSTALL_DIR}/bin/gcc" >> ~/.bashrc
	sudo echo "export CXX=${GCC_INSTALL_DIR}/bin/g++" >> ~/.bashrc
	sudo echo "export LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:$LIBRARY_PATH" >> ~/.bashrc
	sudo echo "export LD_LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:${GCC_INSTALL_DIR}/lib64:\$LD_LIBRARY_PATH" >> ~/.bashrc
	source ~/.bashrc
fi
```

#### About installing `GLIBC_2.18` safely

Running project complied by`gcc-11` may need `GLIBC_2.18`, when your system may have not installed the updated version, you can install it to your local environment using the script.

```shell
PACKAGE_DIR=/path/to/where/tar_package/in
INSTALL_DIR=/path/to/install
GLIBC_DIR=$PACKAGE_DIR/glibc-2.18
GLIBC_INSTALL_DIR=$INSTALL_DIR/glibc-2.18

mkdir -p $GLIBC_INSTALL_DIR
cd $GLIBC_INSTALL_DIR
rm -rf *
cd $PACKAGE_DIR
rm -rf glibc-2.18
if [ ! -f "glibc-2.18.tar.gz" ]; then
	wget --no-check-certificate https://mirrors.tuna.tsinghua.edu.cn/gnu/glibc/glibc-2.18.tar.gz
fi
tar -xvzf glibc-2.18.tar.gz
cd $GLIBC_DIR
./glibc-2.18/configure --prefix=$GLIBC_INSTALL_DIR
make -j6
make install
```

then compile the project with flag `-Wl,--rpath=` and `-Wl,--dynamic-linker=`, when using `CmakeLists.txt`, add the following sentences

```cmake
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--rpath=/path/to/install/glibc-2.18/lib -Wl,--dynamic-linker=/path/to/install/glibc-2.18/lib/ld-2.18.so")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--rpath=/path/to/install/glibc-2.18/lib -Wl,--dynamic-linker=/path/to/install/glibc-2.18/lib/ld-2.18.so")
```

