package dalgo2mongodb

import (
	"context"
	"errors"
)

// Preserve only the public context sentinels, never a driver error or an
// arbitrary context.WithCancelCause payload that can contain credentials/data.
func contextFailure(ctx context.Context, driverErr error) error {
	for _, err := range []error{ctx.Err(), driverErr} {
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
	}
	return nil
}

func sealedReadError(ctx context.Context, driverErr error, message string) error {
	if err := contextFailure(ctx, driverErr); err != nil {
		return err
	}
	return errors.New(message)
}
