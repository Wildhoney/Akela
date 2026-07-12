FROM rust:1.96-slim AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --system --no-create-home akela
COPY --from=builder /app/target/release/akela /usr/local/bin/akela
USER akela
EXPOSE 8080
ENTRYPOINT ["akela"]
