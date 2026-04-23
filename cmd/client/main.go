package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	pb "project21/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "submit":
		err = submitCmd(os.Args[2:])
	case "query":
		err = queryCmd(os.Args[2:])
	case "metrics":
		err = metricsCmd(os.Args[2:])
	case "compare":
		err = compareCmd(os.Args[2:])
	case "validate":
		err = validateCmd(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func submitCmd(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	nodes := fs.String("nodes", "localhost:5001,localhost:5002,localhost:5003", "comma-separated node addresses")
	command := fs.String("command", "", "command to append, for example: SET color blue")
	timeout := fs.Duration("timeout", 3*time.Second, "request timeout")
	_ = fs.Parse(args)
	cmd := strings.TrimSpace(*command)
	if cmd == "" {
		cmd = strings.TrimSpace(strings.Join(fs.Args(), " "))
	}
	if cmd == "" {
		return fmt.Errorf("command is required")
	}

	resp, address, err := submitToCluster(splitCSV(*nodes), cmd, *timeout)
	if err != nil {
		return err
	}
	fmt.Printf("submitted address=%s leader=%s index=%d term=%d ok=%v message=%q\n", address, resp.LeaderId, resp.Index, resp.Term, resp.Ok, resp.Message)
	if !resp.Ok {
		return fmt.Errorf("entry was not committed")
	}
	return nil
}

func queryCmd(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	address := fs.String("addr", "localhost:5001", "node address")
	asJSON := fs.Bool("json", false, "print JSON")
	timeout := fs.Duration("timeout", 2*time.Second, "request timeout")
	_ = fs.Parse(args)

	resp, err := queryLog(*address, *timeout)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp)
	}
	printLog(resp, *address)
	return nil
}

func metricsCmd(args []string) error {
	fs := flag.NewFlagSet("metrics", flag.ExitOnError)
	address := fs.String("addr", "localhost:5001", "node address")
	asJSON := fs.Bool("json", false, "print JSON")
	timeout := fs.Duration("timeout", 2*time.Second, "request timeout")
	_ = fs.Parse(args)

	resp, err := getMetrics(*address, *timeout)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp)
	}
	printMetrics(resp, *address)
	return nil
}

func compareCmd(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	nodes := fs.String("nodes", "localhost:5001,localhost:5002,localhost:5003", "comma-separated node addresses")
	timeout := fs.Duration("timeout", 2*time.Second, "request timeout")
	_ = fs.Parse(args)

	logs, err := collectLogs(splitCSV(*nodes), *timeout)
	if err != nil {
		return err
	}
	equal := logsEqual(logs)
	for address, resp := range logs {
		printLog(resp, address)
	}
	if !equal {
		return fmt.Errorf("logs differ")
	}
	fmt.Println("logs are identical")
	return nil
}

func validateCmd(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	nodes := fs.String("nodes", "localhost:5001,localhost:5002,localhost:5003", "comma-separated node addresses")
	command := fs.String("command", fmt.Sprintf("SET validation %d", time.Now().UnixNano()), "command to submit before validation")
	timeout := fs.Duration("timeout", 3*time.Second, "request timeout")
	wait := fs.Duration("wait", 10*time.Second, "max time to wait for convergence")
	_ = fs.Parse(args)

	addresses := splitCSV(*nodes)
	resp, address, err := submitToCluster(addresses, *command, *timeout)
	if err != nil {
		return err
	}
	fmt.Printf("submitted address=%s leader=%s index=%d term=%d\n", address, resp.LeaderId, resp.Index, resp.Term)

	logs, err := waitForEqualLogs(addresses, *timeout, *wait)
	if err != nil {
		return err
	}
	fmt.Println("logs are identical")
	for address, resp := range logs {
		fmt.Printf("node=%s id=%s role=%s term=%d commit=%d applied=%d entries=%d\n", address, resp.NodeId, resp.Role, resp.CurrentTerm, resp.CommitIndex, resp.LastApplied, len(resp.Entries))
	}

	metrics, err := collectMetrics(addresses, *timeout)
	if err != nil {
		return err
	}
	printApplyDelays(metrics, resp.Index)
	return nil
}

func submitToCluster(addresses []string, command string, timeout time.Duration) (*pb.SubmitEntryResponse, string, error) {
	var lastErr error
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		for _, address := range addresses {
			resp, err := submit(address, command, timeout/2)
			if err != nil {
				lastErr = err
				continue
			}
			if resp.Ok {
				return resp, address, nil
			}
			lastErr = fmt.Errorf("%s: %s", address, resp.Message)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no nodes configured")
	}
	return nil, "", lastErr
}

func submit(address, command string, timeout time.Duration) (*pb.SubmitEntryResponse, error) {
	conn, err := dial(address, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return pb.NewRaftServiceClient(conn).SubmitEntry(ctx, &pb.SubmitEntryRequest{Command: command})
}

func queryLog(address string, timeout time.Duration) (*pb.QueryLogResponse, error) {
	conn, err := dial(address, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return pb.NewRaftServiceClient(conn).QueryLog(ctx, &pb.QueryLogRequest{})
}

func getMetrics(address string, timeout time.Duration) (*pb.GetMetricsResponse, error) {
	conn, err := dial(address, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return pb.NewRaftServiceClient(conn).GetMetrics(ctx, &pb.GetMetricsRequest{})
}

func dial(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func collectLogs(addresses []string, timeout time.Duration) (map[string]*pb.QueryLogResponse, error) {
	logs := make(map[string]*pb.QueryLogResponse, len(addresses))
	for _, address := range addresses {
		resp, err := queryLog(address, timeout)
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", address, err)
		}
		logs[address] = resp
	}
	return logs, nil
}

func collectMetrics(addresses []string, timeout time.Duration) (map[string]*pb.GetMetricsResponse, error) {
	metrics := make(map[string]*pb.GetMetricsResponse, len(addresses))
	for _, address := range addresses {
		resp, err := getMetrics(address, timeout)
		if err != nil {
			return nil, fmt.Errorf("metrics %s: %w", address, err)
		}
		metrics[address] = resp
	}
	return metrics, nil
}

func waitForEqualLogs(addresses []string, timeout, wait time.Duration) (map[string]*pb.QueryLogResponse, error) {
	deadline := time.Now().Add(wait)
	var last map[string]*pb.QueryLogResponse
	var lastErr error
	for time.Now().Before(deadline) {
		logs, err := collectLogs(addresses, timeout)
		if err != nil {
			lastErr = err
			time.Sleep(250 * time.Millisecond)
			continue
		}
		last = logs
		if logsEqual(logs) {
			return logs, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return last, fmt.Errorf("logs did not converge within %s", wait)
}

func logsEqual(logs map[string]*pb.QueryLogResponse) bool {
	var reference string
	first := true
	for _, resp := range logs {
		current := canonicalLog(resp.Entries)
		if first {
			reference = current
			first = false
			continue
		}
		if current != reference {
			return false
		}
	}
	return !first
}

func canonicalLog(entries []*pb.LogEntry) string {
	var b strings.Builder
	for _, entry := range entries {
		fmt.Fprintf(&b, "%d:%d:%s\n", entry.Index, entry.Term, entry.Command)
	}
	return b.String()
}

func printLog(resp *pb.QueryLogResponse, address string) {
	fmt.Printf("\n[%s] node=%s role=%s leader=%s term=%d commit=%d applied=%d\n", address, resp.NodeId, resp.Role, resp.LeaderId, resp.CurrentTerm, resp.CommitIndex, resp.LastApplied)
	for _, entry := range resp.Entries {
		marker := " "
		if entry.Index <= resp.CommitIndex {
			marker = "*"
		}
		fmt.Printf("%s index=%d term=%d command=%q\n", marker, entry.Index, entry.Term, entry.Command)
	}
	if len(resp.State) > 0 {
		keys := make([]string, 0, len(resp.State))
		for key := range resp.State {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Println("state:")
		for _, key := range keys {
			fmt.Printf("  %s=%q\n", key, resp.State[key])
		}
	}
}

func printMetrics(resp *pb.GetMetricsResponse, address string) {
	fmt.Printf("[%s] node=%s role=%s term=%d commit=%d applied=%d replication_lag=%d\n", address, resp.NodeId, resp.Role, resp.CurrentTerm, resp.CommitIndex, resp.LastApplied, resp.ReplicationLag)
	for _, metric := range resp.Metrics {
		fmt.Printf("index=%d submitted=%d committed=%d applied=%d apply_delay_ms=%.3f\n",
			metric.Index,
			metric.SubmittedAtUnixNano,
			metric.CommittedAtUnixNano,
			metric.AppliedAtUnixNano,
			float64(metric.ApplyDelayNanos)/1e6,
		)
	}
}

func printApplyDelays(metrics map[string]*pb.GetMetricsResponse, index int64) {
	var leaderCommit int64
	for _, resp := range metrics {
		if resp.Role != "leader" {
			continue
		}
		for _, metric := range resp.Metrics {
			if metric.Index == index {
				leaderCommit = metric.CommittedAtUnixNano
			}
		}
	}

	for address, resp := range metrics {
		fmt.Printf("metrics node=%s id=%s role=%s replication_lag=%d\n", address, resp.NodeId, resp.Role, resp.ReplicationLag)
		for _, metric := range resp.Metrics {
			if metric.Index != index {
				continue
			}
			if leaderCommit > 0 && metric.AppliedAtUnixNano > 0 {
				fmt.Printf("  index=%d apply_delay_from_leader_ms=%.3f\n", index, float64(metric.AppliedAtUnixNano-leaderCommit)/1e6)
			} else {
				fmt.Printf("  index=%d timing incomplete\n", index)
			}
		}
	}
}

func printJSON(v interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(v)
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  client submit   --nodes localhost:5001,localhost:5002,localhost:5003 --command "SET color blue"
  client query    --addr localhost:5001
  client metrics  --addr localhost:5001
  client compare  --nodes localhost:5001,localhost:5002,localhost:5003
  client validate --nodes localhost:5001,localhost:5002,localhost:5003`)
}
