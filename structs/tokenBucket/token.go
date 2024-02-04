package tokenBucket

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	FIELDSIZE = 8
)

/*
TokenBucket represents a simple rate limiting token bucket.
*/
type TokenBucket struct {
	capacity       int64     // maximum tokens in the bucket
	refillCooldown int64     // user defined cooldown in seconds, until the next refill
	tokens         int64     // current number of tokens in the bucket
	lastRefill     time.Time // when was the number of tokens reset
}

/*
MakeTokenBucket creates a new TokenBucket with the specified capacity and refill cooldown.
*/
func MakeTokenBucket(capacity int64, refillCooldown int64) *TokenBucket {
	return &TokenBucket{
		capacity:       capacity,
		refillCooldown: refillCooldown,
		tokens:         capacity,
		lastRefill:     time.Now(),
	}
}

/*
refill resets the token bucket to its maximum capacity and updates the last refill time.
*/
func (tb *TokenBucket) refill() {
	tb.tokens = tb.capacity
	tb.lastRefill = time.Now()
}

/*
TakeToken attempts to consume the specified number of tokens from the bucket.
It returns the serialized data representing the state after token consumption,
or an error if the request cannot be fulfilled due to rate limiting.
*/
func (tb *TokenBucket) TakeToken(tokens int64) ([]byte, error) {
	if tb.lastRefill.Add(time.Duration(tb.refillCooldown) * time.Second).Before(time.Now()) {
		tb.refill()
	}
	if tb.tokens <= 0 {
		return nil, errors.New("reached maximum number of requests, please wait")
	}
	tb.tokens -= tokens
	serializedData := tb.TokenRequestToBytes()
	return serializedData, nil
}

/*
TokenRequestToBytes converts the current state of the TokenBucket to a serialized byte slice.
*/
func (tb *TokenBucket) TokenRequestToBytes() []byte {
	data := make([]byte, 24)

	binary.BigEndian.PutUint64(data[:2*FIELDSIZE], uint64(tb.lastRefill.UnixNano()))
	binary.BigEndian.PutUint64(data[2*FIELDSIZE:3*FIELDSIZE], uint64(tb.tokens))

	return data
}

/*
BytesToTokenRequest updates the TokenBucket state by deserializing the provided byte slice.
*/
func (tb *TokenBucket) BytesToTokenRequest(data []byte) {
	tb.lastRefill = time.Unix(0, int64(binary.BigEndian.Uint64(data[:2*FIELDSIZE])))
	tb.tokens = int64(binary.BigEndian.Uint64(data[2*FIELDSIZE : 3*FIELDSIZE]))
}
