package reqbound

import (
	"context"
	"errors"
	"github.com/lefalya/item"
	"github.com/redis/go-redis/v9"
	"time"
)

type Reqbound[T item.Blueprint] struct {
	client     redis.UniversalClient
	name       string
	throughput int64 // number of events per minute
	duration   time.Duration
}

func (eq *Reqbound[T]) Add(ctx context.Context, item T) error {
	err := eq.client.LPush(ctx, eq.name, item.GetRandId())
	if err != nil {
		return err.Err()
	}

	return nil
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

		go func(ranID string) {
			err = processor(randid)
			if err != nil {
				errMsg := errors.New("Invocation RandId: " + randid + " failed: " + err.Error())
				errorLogger(errMsg, randid)
				errPush := eq.client.LPush(ctx, eq.name, randid)
				if errPush.Err() != nil {
					errorLogger(errors.New("Failed to push back randId: "+randid+" error: "+errPush.Err().Error()), randid)
				}
			}
		}(randid)
	}
}

func NewEventQueue[T item.Blueprint](redis *redis.UniversalClient, name string, throughput int64) *Reqbound[T] {
	duration := time.Duration(60/throughput) * time.Second

	return &Reqbound[T]{
		client:     *redis,
		name:       name,
		throughput: throughput,
		duration:   duration,
	}
}
