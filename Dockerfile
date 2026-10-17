FROM golang:1.26.9 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /migres ./cmd/migres
RUN mkdir -p /empty

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /migres /usr/local/bin/migres
COPY --from=build --chown=65532:65532 /empty /app/data
WORKDIR /app
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/migres"]
CMD ["run", "--config", "/app/config.yml"]
