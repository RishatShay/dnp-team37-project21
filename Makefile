.PHONY: build up down logs submit query compare metrics validate crash restart clean proto

NODES ?= node1:50051,node2:50051,node3:50051
HOST_NODES ?= localhost:5001,localhost:5002,localhost:5003
CMD ?= SET demo hello

build:
	docker compose build

up:
	docker compose up -d --build node1 node2 node3

down:
	docker compose down

logs:
	docker compose logs -f node1 node2 node3

submit:
	docker compose run --rm client submit --nodes "$(NODES)" --command "$(CMD)"

query:
	docker compose run --rm client query --addr node1:50051

compare:
	docker compose run --rm client compare --nodes "$(NODES)"

metrics:
	docker compose run --rm client metrics --addr node1:50051

validate:
	docker compose run --rm client validate --nodes "$(NODES)" --command "$(CMD)"

crash:
	docker compose stop node2

restart:
	docker compose start node2

clean:
	docker compose down -v

proto:
	rm -f internal/pb/raft.pb.go internal/pb/raft_grpc.pb.go
	protoc -I proto \
			--go_out=internal/pb --go_opt=paths=source_relative \
			--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
			raft.proto