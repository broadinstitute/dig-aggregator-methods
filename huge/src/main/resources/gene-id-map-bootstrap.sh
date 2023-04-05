
# Install prerequisites
sudo yum list "*rpm*"
sudo yum install -y git rpm-build

# Install Rust toolchain
mkdir /mnt/rust
mkdir /mnt/rust/rustup
mkdir /mnt/rust/cargo
export RUSTUP_HOME=/mnt/rust/rustup
export CARGO_HOME=/mnt/rust/cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain 1.67.0 -y
export PATH=$PATH:$CARGO_HOME/bin
bash $CARGO_HOME/env

# Install eugene
mkdir git
cd git || exit
git clone https://github.com/broadinstitute/eugene.git
cd eugene || exit
git checkout v0.1.0
export PATH=$PATH:$HOME/.cargo/bin
cargo install cargo-rpm
cargo rpm init
cargo rpm build -v
ls -ralt ./target/release/
ls -ralt ./target/release/rpmbuild
ls -ralt ./target/release/rpmbuild/RPMS/
ls -ralt ./target/release/rpmbuild/RPMS/x86_64/
sudo yum install -y ./target/release/rpmbuild/RPMS/x86_64/eugene-0.1.0-1.amzn2.x86_64.rpm
cd ../..
rm -r git
df -h
echo "Done bootstrapping!"

