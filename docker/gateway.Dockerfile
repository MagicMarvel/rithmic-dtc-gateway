FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates curl libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.91.0
ENV PATH=/root/.cargo/bin:$PATH
WORKDIR /source
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY web-terminal ./web-terminal
RUN cargo build --release --bin dtc_server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /source/target/release/dtc_server /usr/local/bin/rithmic-dtc-gateway
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/rithmic-dtc-gateway"]
