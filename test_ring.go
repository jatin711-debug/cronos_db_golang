package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

func main() {
	ring := make([]uint64, 0)
	nodeMap := make(map[uint64]string)

	addNode := func(nodeID string) {
		for i := 0; i < 2048; i++ {
			key := nodeID + "-" + strconv.Itoa(i)
			hasher := fnv.New64a()
			hasher.Write([]byte(key))
			hash := hasher.Sum64()
			ring = append(ring, hash)
			nodeMap[hash] = nodeID
		}
	}

	addNode("node1")
	addNode("node2")
	addNode("node3")

	sort.Slice(ring, func(i, j int) bool { return ring[i] < ring[j] })

	counts := make(map[string]int)
	for i := 0; i < 16; i++ {
		key := "partition-" + strconv.Itoa(i)
		hasher := fnv.New64a()
		hasher.Write([]byte(key))
		hash := hasher.Sum64()

		idx := sort.Search(len(ring), func(j int) bool { return ring[j] >= hash })
		if idx >= len(ring) {
			idx = 0
		}
		owner := nodeMap[ring[idx]]
		counts[owner]++
	}
	fmt.Printf("%v\n", counts)
}
