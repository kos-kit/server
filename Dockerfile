FROM --platform=$BUILDPLATFORM rust:1-bullseye as builder
ARG BUILDARCH TARGETARCH
RUN apt-get update && \
    apt-get install -y libclang-dev clang && \
    if [ "$BUILDARCH" != "$TARGETARCH" ] && [ "$TARGETARCH" = "arm64" ] ; \
    then \
        apt-get install -y g++-aarch64-linux-gnu && \
        rustup target add aarch64-unknown-linux-gnu ; \
    fi
COPY . /server
WORKDIR /server
RUN if [ "$BUILDARCH" != "$TARGETARCH" ] && [ "$TARGETARCH" = "arm64" ] ; \
    then \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
        export BINDGEN_EXTRA_CLANG_ARGS="--sysroot /usr/aarch64-linux-gnu" && \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        mv /server/target/aarch64-unknown-linux-gnu/release/kos-kit-server /server/target/release/kos-kit-server ; \
    else \
        cargo build --release ; \
    fi

FROM --platform=$TARGETPLATFORM gcr.io/distroless/cc-debian11
COPY --from=builder /server/target/release/kos-kit-server /usr/local/bin/kos-kit-server
ENTRYPOINT [ "/usr/local/bin/kos-kit-server" ]
CMD [ "--location", "/data", "--bind", "0.0.0.0:7878" ]
