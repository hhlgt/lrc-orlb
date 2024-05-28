CRT_DIR=$(pwd)
set -e

SOCKET_RPC_INSTALL_DIR=$CRT_DIR"/project/third_party/yalantinglibs"
GF_INSTALL_DIR=$CRT_DIR"/project/third_party/gf-complete"
JERASURE_INSTALL_DIR=$CRT_DIR"/project/third_party/jerasure"
HIREDIS_INSTALL_DIR=$CRT_DIR"/project/third_party/hiredis"
REDIS_PLUS_PLUS_INSTALL_DIR=$CRT_DIR"/project/third_party/redis-plus-plus"
REDIS_INSTALL_DIR=$CRT_DIR"/project/third_party/redis"

SOCKET_RPC_DIR=$CRT_DIR"/third_party/yalantinglibs"
GF_DIR=$CRT_DIR"/third_party/gf-complete"
JERASURE_DIR=$CRT_DIR"/third_party/Jerasure"
HIREDIS_DIR=$CRT_DIR"/third_party/hiredis"
REDIS_PLUS_PLUS_DIR=$CRT_DIR"/third_party/redis-plus-plus"
REDIS_DIR=$CRT_DIR"/third_party/redis"

mkdir -p $CRT_DIR"/third_party"

# socket & rpc
sudo apt-get install libprotobuf-dev protobuf-compiler libprotoc-dev
mkdir -p $SOCKET_RPC_INSTALL_DIR
cd $SOCKET_RPC_INSTALL_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf yalantinglibs
git clone git@github.com:alibaba/yalantinglibs.git
cd $SOCKET_RPC_DIR
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$SOCKET_RPC_INSTALL_DIR
cmake --install .

# gf-complete
mkdir -p $GF_INSTALL_DIR
cd $GF_INSTALL_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf gf-complete
git clone git@github.com:ceph/gf-complete.git
cd $GF_DIR
autoreconf -if
./configure --prefix=$GF_INSTALL_DIR
make -j6
make install

# jerasure
mkdir -p $JERASURE_INSTALL_DIR
cd $JERASURE_INSTALL_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf Jerasure
git clone git@github.com:tsuraan/Jerasure.git
cd $JERASURE_DIR
autoreconf -if
./configure --prefix=$JERASURE_INSTALL_DIR LDFLAGS=-L$GF_INSTALL_DIR/lib CPPFLAGS=-I$GF_INSTALL_DIR/include
make -j6
make install

# hiredis
mkdir -p $HIREDIS_INSTALL_DIR
cd $HIREDIS_INSTALL_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf hiredis
git clone git@github.com:redis/hiredis.git
cd $HIREDIS_DIR
make PREFIX=$HIREDIS_INSTALL_DIR
make PREFIX=$HIREDIS_INSTALL_DIR install
# set the load path
# echo 'export LD_LIBRARY_PATH='$HIREDIS_INSTALL_DIR'/lib:$LD_LIBRARY_PATH' >> ~/.bashrc
# source ~/.bashrc

# redis-plus-plus
mkdir -p $REDIS_PLUS_PLUS_INSTALL_DIR
cd $REDIS_PLUS_PLUS_INSTALL_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf redis-plus-plus
git clone git@github.com:sewenew/redis-plus-plus.git
cd $REDIS_PLUS_PLUS_DIR
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$HIREDIS_INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$REDIS_PLUS_PLUS_INSTALL_DIR ..
cmake --build . -j8
cmake --install .

# redis
mkdir -p $REDIS_DIR
cd $REDIS_DIR
rm * -rf
cd $CRT_DIR"/third_party"
rm -rf redis
git clone git@github.com:redis/redis.git
cd $REDIS_DIR
make PREFIX=$REDIS_INSTALL_DIR install

rm -rf $CRT_DIR"/third_party"
