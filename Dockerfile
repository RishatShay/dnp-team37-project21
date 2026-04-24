FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/node ./cmd/node
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

FROM debian:bookworm-slim

COPY --from=build /out/node /bin/node
COPY --from=build /out/client /bin/client

ENTRYPOINT ["/bin/node"]
