package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	pb "project21/internal/pb"
	"project21/internal/raft"

	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", getenv("NODE_ID", "node1"), "node id")
	address := flag.String("addr", getenv("RAFT_ADDR", ":50051"), "gRPC listen address")
	dataDir := flag.String("data", getenv("DATA_DIR", "/data"), "data directory")
	peersRaw := flag.String("peers", getenv("PEERS", "node1=localhost:5001,node2=localhost:5002,node3=localhost:5003"), "comma-separated id=address peers")
	flag.Parse()

	store, err := raft.OpenLogStore(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		log.Fatalf("open log store: %v", err)
	}
	defer store.Close()

	node, err := raft.NewNode(raft.Config{
		ID:      *id,
		Address: *address,
		Peers:   parsePeers(*peersRaw),
	}, store, raft.NewStateMachine(), raft.NewMetricsCollector())
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	node.Start(ctx)

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatalf("listen %s: %v", *address, err)
	}

	server := grpc.NewServer()
	pb.RegisterRaftServiceServer(server, node)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Printf("node=%s listening=%s data=%s peers=%s", *id, *address, *dataDir, *peersRaw)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve grpc: %v", err)
	}
}

func parsePeers(raw string) map[string]string {
	peers := make(map[string]string)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, address, ok := strings.Cut(item, "=")
		if !ok {
			log.Fatalf("invalid peer %q, expected id=address", item)
		}
		id = strings.TrimSpace(id)
		address = strings.TrimSpace(address)
		if id == "" || address == "" {
			log.Fatalf("invalid peer %q, id and address are required", item)
		}
		peers[id] = address
	}
	return peers
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("pid=%d ", os.Getpid()))
}
