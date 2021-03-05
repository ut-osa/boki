package sync

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type ShardedQueue struct {
	subQueues []*Queue

	shards   []int
	nextPush int
	nextPop  int
}

func NewShardedQueue(ctx context.Context, env types.Environment, name string, shards int) (*ShardedQueue, error) {
	if shards <= 0 {
		log.Fatalf("[FATAL] Shards must be positive!")
	}
	queues := make([]*Queue, 0, 16)
	for i := 0; i < shards; i++ {
		q, err := NewQueue(ctx, env, fmt.Sprintf("%s-%d", name, i))
		if err != nil {
			return nil, err
		}
		queues = append(queues, q)
	}
	shardArr := make([]int, shards)
	for i := 0; i < shards; i++ {
		shardArr[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(shards, func(i, j int) {
		shardArr[i], shardArr[j] = shardArr[j], shardArr[i]
	})
	return &ShardedQueue{
		subQueues: queues,
		shards:    shardArr,
		nextPush:  rand.Intn(shards),
		nextPop:   rand.Intn(shards),
	}, nil
}

func (q *ShardedQueue) Push(payload string) error {
	shard := q.shards[q.nextPush]
	q.nextPush = (q.nextPush + 1) % len(q.shards)
	queue := q.subQueues[shard]
	return queue.Push(payload)
}

func (q *ShardedQueue) Pop() (string /* payload */, error) {
	shard := q.shards[q.nextPop]
	q.nextPop = (q.nextPop + 1) % len(q.shards)
	queue := q.subQueues[shard]
	return queue.Pop()
}

func (q *ShardedQueue) PopBlocking() (string /* payload */, error) {
	shard := q.shards[q.nextPop]
	q.nextPop = (q.nextPop + 1) % len(q.shards)
	queue := q.subQueues[shard]
	return queue.PopBlocking()
}

func (q *ShardedQueue) PopFromShard(shard int) (string /* payload */, error) {
	if 0 <= shard && shard < len(q.subQueues) {
		return q.subQueues[shard].Pop()
	} else {
		return "", fmt.Errorf("Invalid shard number: %d", shard)
	}
}
