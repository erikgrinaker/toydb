# Initial build
FROM rust:1.37 AS build

RUN apt-get update \
    && apt-get install -y musl-tools protobuf-compiler \
    && rustup target add x86_64-unknown-linux-musl

ENV RUSTFLAGS=-Clinker=musl-gcc
WORKDIR /usr/src/toydb

# FIXME: cargo does not have an option to only build dependencies, so we build
# a dummy main.rs. See: https://github.com/rust-lang/cargo/issues/2644
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" >src/main.rs \
    && echo "fn main() {}" >build.rs
RUN cargo fetch --target=x86_64-unknown-linux-musl
RUN cargo build --release --target=x86_64-unknown-linux-musl \
    && rm -rf build.rs src target/x86_64-unknown-linux-musl/release/toydb*

COPY . .
RUN cargo build --release --target=x86_64-unknown-linux-musl \
    && cargo install --path . --target=x86_64-unknown-linux-musl

# Runtime image
FROM alpine:3.9
COPY --from=build /usr/local/cargo/bin/toydb /usr/local/bin/toydb
COPY --from=build /usr/src/toydb/config/toydb.yaml /etc/toydb.yaml
CMD ["toydb"]