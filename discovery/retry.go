package discovery

import (
	"context"
	"fmt"
	"time"
)

func retryTimeout(ctx context.Context, interval, timeout time.Duration, fn func() error) error {
	timeoutTimer := time.After(timeout)
	for {
		err := fn()
		if err == nil {
			// success
			return nil
		}

		intervalTimer := time.After(interval)
		// Wait for interval, timeout, or context is cancelled.
	inner:
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled")
			case <-intervalTimer:
				break inner
			case <-timeoutTimer:
				return fmt.Errorf("timeout of %s reached: %w", timeout, err)
			}
		}
	}
}

func retryForever(ctx context.Context, interval time.Duration, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			// success
			return nil
		}

		intervalTimer := time.After(interval)
		// Wait for interval, timeout, or context is cancelled.
	inner:
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled")
			case <-intervalTimer:
				break inner
			}
		}
	}
}
