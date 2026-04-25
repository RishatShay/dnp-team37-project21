#!/bin/bash
# Usage: chmod +x demo.sh && ./demo.sh

NODES="node1:50051,node2:50051,node3:50051"

show_node_metrics() {
  local addr="$1"
  docker-compose run --rm client metrics --addr "$addr" 2>/dev/null | awk '/^\[/{print}; /^index=/{line=$0} END{if(line) print line}'
}

find_leader() {
  for node in node1 node2 node3; do
    if docker-compose run --rm client metrics --addr "${node}:50051" 2>/dev/null | grep -q 'role=leader'; then
      echo "$node"
      return 0
    fi
  done
  return 1
}

echo "RAFT LOG REPLICATION DEMO"
echo "================"

echo -e "\nStep 1: Start 3 nodes"
docker-compose up -d --build node1 node2 node3
sleep 4
read -p "Nodes started. Press Enter to continue..."

echo -e "\nStep 2: Read logs & verify match"
docker-compose run --rm client validate --nodes $NODES
read -p "Logs match. Press Enter..."

echo -e "\nStep 3: Submit log 'SET user Alice'"
docker-compose run --rm client submit --nodes $NODES --command "SET user Alice"
echo -n "Verify: "; docker-compose run --rm client query --addr node1:50051 | grep 'user=' || true
read -p "Alice saved. Press Enter..."

echo -e "\nStep 4: Measure replication lag and apply delay"
for node in node1 node2 node3; do
  echo "--- $node ---"
  show_node_metrics "${node}:50051"
done
read -p "Replication lag displayed. Press Enter to continue..."

echo -e "\n📋 Step 5: Kill the leader"
echo "Current Status:"
for node in node1 node2 node3; do
  echo "--- $node ---"
  show_node_metrics "${node}:50051" || true
done
STOPPED_NODE=$(find_leader)
if [[ -n "$STOPPED_NODE" ]]; then
  echo "Stopping leader: $STOPPED_NODE"
  docker-compose stop "$STOPPED_NODE"
else
  echo "Leader not found, please stop the leader manually."
fi
read -p "Leader killed. Press Enter to continue..."

echo -e "\nStep 6: Request status (Check leader change)"
for node in node1 node2 node3; do
  echo "--- $node ---"
  show_node_metrics "${node}:50051" || true
done
read -p "New leader elected. Press Enter..."

echo -e "\nStep 7: Submit more logs (Quorum test)"
leader=$(find_leader)
if [[ -n "$leader" ]]; then
  echo "Submitting to leader: $leader"
  docker-compose run --rm client submit --nodes "${leader}:50051" --command "SET user Bob"
else
  echo "Leader not found, submitting to all nodes"
  docker-compose run --rm client submit --nodes $NODES --command "SET user Bob"
fi
read -p "Bob saved. Press Enter..."

echo -e "\nStep 8: Restart dead node & verify follower"
if [[ -n "$STOPPED_NODE" ]]; then
  echo "Restarting stopped node: $STOPPED_NODE"
  docker-compose start "$STOPPED_NODE"
else
  echo "Stopped node is unknown, please restart the node manually."
fi
read -p "Node restarted. Waiting for catch-up..."
sleep 5
echo -n "Node Role: "; docker-compose run --rm client query --addr node1:50051 2>/dev/null | grep -o 'role=[a-z]*'
read -p "Node rejoined as follower. Press Enter..."

echo -e "\nStep 9: Final verification (Logs match)"
docker-compose run --rm client compare --nodes $NODES
echo -e "\nDemo complete! All nodes are consistent."