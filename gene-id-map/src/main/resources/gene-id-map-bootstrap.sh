
# Install prerequisites
sudo yum install -y curl git build-essential pkg-config

# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain 1.67.0 -y
export PATH=$PATH:$HOME/.cargo/bin
bash $HOME/.cargo/env

# Install eugene
mkdir git
cd git || exit
git clone https://github.com/broadinstitute/eugene.git
cd eugene || exit
git checkout v0.1.0
export PATH=$PATH:$HOME/.cargo/bin
cargo install cargo-rpm
cargo rpm init
cargo rpm build
ls -ralt ./target/release/
ls -ralt ./target/release/RPMs/
#sudo yum install -y ./target/release/rpmbuild/RPMs/
cd ../..
rm -r git


