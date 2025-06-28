#!/bin/bash

# Install

echo "Set Timezone"
export DEBIAN_FRONTEND=noninteractive
export TZ=Etc/UTC
sudo ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

echo "Install Rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rust-init.sh
bash rust-init.sh -y
source ~/.cargo/env

echo "Install Dependencies"
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get update
sudo apt-get install -y \
     gcc protobuf-compiler \
     libprotobuf-dev \
     pkg-config \
     libssl-dev \
     python3.11 \
     python3.11-dev \
     python3.11-venv \
     python3.11-distutils

echo "Set Python alternatives"
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
     sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
     curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

echo "Install Python packages"
pip install --break-system-packages --upgrade setuptools wheel
pip install --break-system-packages pyspark-client==4.0.0 pandas psutil

echo "Install Sail v0.3.0"
git clone https://github.com/lakehq/sail.git
cd sail/
git checkout v0.3.0
RUSTFLAGS="-C target-cpu=native" cargo build --release --package sail-cli --bins
export PATH="`pwd`/target/release:$PATH"
cd ..


# Load the data

echo "Download benchmark target data, single file"
wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.parquet'

#echo "Download benchmark target data, partitioned"
#mkdir -p partitioned
#seq 0 99 | xargs -P100 -I{} bash -c 'wget --directory-prefix partitioned --continue https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet'

# Run the queries

echo "Run benchmarks for single parquet"
./run.sh 2>&1 | tee "sail_log.txt"

cat sail_log.txt | grep -P '^Time:\s+([\d\.]+)|Failure!' | sed -r -e 's/Time: //; s/^Failure!$/null/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
