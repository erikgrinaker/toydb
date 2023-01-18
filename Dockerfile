# Initial build
FROM rust:1.66-slim AS build

ARG TARGET=x86_64-unknown-linux-musl
RUN apt-get -q update && apt-get -q install -y musl-dev
RUN rustup target add $TARGET

# FIXME: cargo does not have an option to only build dependencies, so we build
# a dummy main.rs. See: https://github.com/rust-lang/cargo/issues/2644
WORKDIR /usr/src/toydb

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" >src/main.rs \
    && echo "fn main() {}" >build.rs
RUN cargo fetch --target=$TARGET
RUN cargo build --release --target=$TARGET \
    && rm -rf build.rs src target/$TARGET/release/toydb*

COPY . .
RUN cargo install --bin toydb --locked --offline --path . --target=$TARGET

# Runtime image
FROM alpine:3.17
COPY --from=build /usr/local/cargo/bin/toydb /usr/local/bin/toydb
COPY --from=build /usr/src/toydb/config/toydb.yaml /etc/toydb.yaml
CMD ["toydb"]
