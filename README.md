# Memento

Memento is a tool used to load and save accounts from old Solana snapshots in Google Cloud Storage.

## Why should I use Memento?
- No fighting solana versioning between your geyser plugin and the validator.
- No fighting the rust compiler between your geyser plugin and the validator.
- Easy programmatic access to bulk snapshot ingestion.

# Developing

### Build
```bash
$ cargo b --release
```

### Run

**Worker**
```bash
$ ./target/release/jito-workers --help
```

**Backfill**

The following is a backfill client that backfills to clickhouse.
```bash
$ ./target/release/jito-replayer-backfill --help
```

# How does it work
- There's a server and client. 
- When starting up the server, it will load in all snapshots available across all GCP buckets you provide on the CLI.
- The clients can connect to the server over GRPC to start backfills and request information on the snapshots available.

# System Setup
Note: During the development a machine with the following specs was used:
- CPU: AMD Epyc 7313P
- Cores/Threads: 16c / 32t
- RAM: 128 GB
- Drive 0: NVME0N1: 15.36 TB
- Drive 1: NVME1N1: 15.36 TB
- Drive 2: NVME2N1: 15.36 TB

Drive 1 and 2 were mounted as an NVME in RAID0 (directions below).

You can get away with NVMEs if you're dumping to an external database. Also, you should tune the number of workers based on your system memory.

## Steps

Kernel modifications for validator. (Some might be unnecessary.)

```bash
$ sudo bash -c "cat >/etc/sysctl.d/21-solana-validator.conf <<EOF
# Increase UDP buffer sizes
net.core.rmem_default = 134217728
net.core.rmem_max = 134217728
net.core.wmem_default = 134217728
net.core.wmem_max = 134217728

# Increase memory mapped files limit
vm.max_map_count = 10000000
EOF"

$ sudo bash -c "cat >/etc/security/limits.d/90-solana-nofiles.conf <<EOF
# Increase process file descriptor count limit
* - nofile 1000000
EOF"

$ sudo sysctl -p /etc/sysctl.d/21-solana-validator.conf
```

Install packages
```bash
$ sudo apt update
$ sudo apt install -y \
      unzip \
      mdadm \
      libssl-dev \
      libudev-dev \
      pkg-config \
      zlib1g-dev \
      llvm \
      clang \
      cmake \
      make \
      libprotobuf-dev \
      protobuf-compiler \
      libpq-dev \
      nload \
      iotop \
      apt-transport-https \
      ca-certificates \
      dirmngr \
      --no-install-recommends
```

Install protobufs

```
export PROTOC_VERSION=21.12 && \
        export PROTOC_ZIP=protoc-$PROTOC_VERSION-linux-x86_64.zip && \
        curl -Ss -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
        && sudo unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
        && sudo unzip -o $PROTOC_ZIP -d /usr/local include/* \
        && rm -f $PROTOC_ZIP
```

Install rust:
```bash
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
$ source "$HOME/.cargo/env"
```

Mount NVMEs and setup swap (your system may differ)
```bash
$ sudo mdadm --create /dev/md0 --level=0 --raid-devices=2 \
 /dev/nvme1n1 \
 /dev/nvme2n1
$ sudo mkfs.ext4 -F /dev/md0
$ sudo mkdir -p /mnt/disks/ledger
$ sudo mount /dev/md0 /mnt/disks/ledger
$ sudo chmod a+w /mnt/disks/ledger

$ sudo fallocate -l 512G /swapfile
$ sudo chmod 600 /swapfile
$ sudo mkswap /swapfile
$ sudo swapon /swapfile
$ sudo swapon --show
```

Setup and install clickhouse.

```bash
$ GNUPGHOME=$(mktemp -d)
$ sudo GNUPGHOME="$GNUPGHOME" gpg --no-default-keyring --keyring /usr/share/keyrings/clickhouse-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8919F6BD2B48D754
$ sudo rm -r "$GNUPGHOME"
$ sudo chmod +r /usr/share/keyrings/clickhouse-keyring.gpg
$ echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
$ sudo apt-get update
$ sudo apt-get install -y clickhouse-server clickhouse-client
$ sudo systemctl start clickhouse-server
```
