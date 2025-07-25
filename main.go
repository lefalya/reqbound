package reqbound

import (
	"context"
	"errors"
	"time"

	"github.com/lefalya/item"
	"github.com/redis/go-redis/v9"
)

type Reqbound[T item.Blueprint] struct {
	client     redis.UniversalClient
	name       string
	throughput int64 // number of events per minute
	duration   time.Duration
}

func (eq *Reqbound[T]) Add(ctx context.Context, item T) error {
	cmd := eq.client.LPush(ctx, eq.name, item.GetRandId())
	return cmd.Err()
}

func (eq *Reqbound[T]) Worker(ctx context.Context, processor func(string) error, errorLogger func(error, string)) {
	ticker := time.NewTicker(eq.duration)
	defer ticker.Stop()

	for range ticker.C {
		randid, err := eq.client.RPop(ctx, eq.name).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			errorLogger(err, "")
			continue
		}

		go func(randID string) {
			err := processor(randID) // Define err within the goroutine scope
			if err != nil {
				errMsg := errors.New("Invocation RandId: " + randID + " failed: " + err.Error())
				errorLogger(errMsg, randID)
				errPush := eq.client.LPush(ctx, eq.name, randID)
				if errPush.Err() != nil {
					errorLogger(errors.New("Failed to push back randId: "+randID+" error: "+errPush.Err().Error()), randID)
				}
			}
		}(randid)
	}
}

func NewReqbound[T item.Blueprint](redis redis.UniversalClient, name string, throughput int64) *Reqbound[T] {
	duration := time.Duration(60/throughput) * time.Second

	return &Reqbound[T]{
		client:     redis,
		name:       name,
		throughput: throughput,
		duration:   duration,
	}
}
