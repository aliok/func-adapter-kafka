/*
Copyright 2026 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Delivery retry defaults. These reproduce the runtime's original hardcoded
// behavior — unlimited retries with exponential backoff from 100ms to 30s — so a
// deployment that sets none of the FUNCTION_DELIVERY_* variables behaves exactly
// as before.
const (
	// defaultDeliveryRetry is the retry count when FUNCTION_DELIVERY_RETRY is
	// unset. A negative value means unlimited: retry until the record is
	// delivered or the session is cancelled, preserving at-least-once with no
	// silent drop. This is the default so existing deployments are unaffected.
	defaultDeliveryRetry   = -1
	defaultBackoffPolicy   = backoffExponential
	defaultBackoffDelay    = 100 * time.Millisecond
	defaultBackoffMaxDelay = 30 * time.Second
)

// backoffPolicy selects how the inter-retry delay grows.
type backoffPolicy string

const (
	backoffExponential backoffPolicy = "exponential"
	backoffLinear      backoffPolicy = "linear"
)

// deliveryConfig controls how the consumer retries delivering a record to the
// function. Build it with loadDeliveryConfig; the zero value is not meaningful.
//
// Field names and semantics mirror Knative's DeliverySpec, with one documented
// difference: when retry is unset the default is unlimited (at-least-once),
// whereas DeliverySpec defaults to zero retries.
type deliveryConfig struct {
	// retry is the number of retries AFTER the first attempt (DeliverySpec
	// semantics): total attempts = retry + 1. A negative value means unlimited.
	retry int
	// policy is the backoff growth policy (exponential or linear).
	policy backoffPolicy
	// baseDelay is the base inter-retry delay.
	baseDelay time.Duration
	// maxDelay caps the computed inter-retry delay. Required because the default
	// is unlimited retries, so an uncapped exponential delay would grow without
	// bound.
	maxDelay time.Duration
}

// defaultDeliveryConfig returns the config used when no FUNCTION_DELIVERY_*
// variables are set: unlimited exponential retries from 100ms to 30s, reproducing
// the runtime's original hardcoded behavior.
func defaultDeliveryConfig() deliveryConfig {
	return deliveryConfig{
		retry:     defaultDeliveryRetry,
		policy:    defaultBackoffPolicy,
		baseDelay: defaultBackoffDelay,
		maxDelay:  defaultBackoffMaxDelay,
	}
}

// unlimited reports whether retries are unbounded.
func (c deliveryConfig) unlimited() bool { return c.retry < 0 }

// maxAttempts returns the total number of delivery attempts (first attempt plus
// retries), or -1 when unlimited.
func (c deliveryConfig) maxAttempts() int {
	if c.unlimited() {
		return -1
	}
	return c.retry + 1
}

// backoffFor returns the delay to wait after the n-th failed attempt (n is
// 1-based) before the next attempt, capped at maxDelay. For exponential the
// delay is baseDelay * 2^(n-1); for linear it is baseDelay * n.
func (c deliveryConfig) backoffFor(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if c.baseDelay <= 0 {
		return 0
	}

	switch c.policy {
	case backoffLinear:
		// baseDelay * n, capped. Compare n against the quotient maxDelay/baseDelay
		// (not quotient+1) so the comparison itself cannot overflow for a tiny base
		// and a huge cap. Once n exceeds that quotient the product would exceed the
		// cap; below it the product is <= maxDelay and cannot overflow.
		if q := c.maxDelay / c.baseDelay; time.Duration(n) > q {
			return c.maxDelay
		}
		d := c.baseDelay * time.Duration(n)
		if d > c.maxDelay {
			return c.maxDelay
		}
		return d
	default: // exponential
		// baseDelay * 2^(n-1), capped. Saturate BEFORE doubling: if the current
		// delay already exceeds half the cap, the next double would meet or exceed
		// the cap anyway, so return it without performing a multiplication that
		// could overflow time.Duration for a tiny base and a huge cap.
		d := c.baseDelay
		for i := 1; i < n; i++ {
			if d > c.maxDelay/2 {
				return c.maxDelay
			}
			d *= 2
		}
		if d > c.maxDelay {
			return c.maxDelay
		}
		return d
	}
}

// loadDeliveryConfig reads the FUNCTION_DELIVERY_* environment variables into a
// deliveryConfig. Each variable is optional; an unset or blank variable falls
// back to the default that reproduces the runtime's original behavior. An
// invalid value is a startup error rather than a silently ignored setting.
func loadDeliveryConfig() (deliveryConfig, error) {
	c := defaultDeliveryConfig()

	// FUNCTION_DELIVERY_RETRY is read with a bare LookupEnv rather than
	// lookupNonEmptyEnv -- deliberately stricter than the other variables. Its
	// default (omitted) means UNLIMITED, the unbounded mode the negative-value
	// rejection below already guards against. Treating a present-but-empty value
	// (e.g. an unrendered template FUNCTION_DELIVERY_RETRY=) as "omitted" would
	// silently re-enable unlimited delivery, so an empty/blank value is a startup
	// error. The benign variables keep their empty-is-default leniency.
	if raw, ok := os.LookupEnv("FUNCTION_DELIVERY_RETRY"); ok {
		v := strings.TrimSpace(raw)
		if v == "" {
			return deliveryConfig{}, fmt.Errorf("invalid FUNCTION_DELIVERY_RETRY %q: must not be empty (omit the variable for unlimited retries)", raw)
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return deliveryConfig{}, fmt.Errorf("invalid FUNCTION_DELIVERY_RETRY %q: %w", raw, err)
		}
		// A negative env value is rejected: unlimited is expressed by omitting
		// the variable, not by passing -1, so a typo cannot silently mean
		// "retry forever".
		if n < 0 {
			return deliveryConfig{}, fmt.Errorf("invalid FUNCTION_DELIVERY_RETRY %q: must not be negative (omit the variable for unlimited retries)", raw)
		}
		// maxAttempts() computes retry+1; math.MaxInt would overflow to a negative
		// attempt budget and drop the first failed record. Reject it rather than
		// silently invert the budget (an effectively-unlimited value this large
		// should omit the variable anyway).
		if n == math.MaxInt {
			return deliveryConfig{}, fmt.Errorf("invalid FUNCTION_DELIVERY_RETRY %q: too large (omit the variable for unlimited retries)", raw)
		}
		c.retry = n
	}

	if v, ok := lookupNonEmptyEnv("FUNCTION_DELIVERY_BACKOFF_POLICY"); ok {
		switch strings.ToLower(v) {
		case string(backoffExponential):
			c.policy = backoffExponential
		case string(backoffLinear):
			c.policy = backoffLinear
		default:
			return deliveryConfig{}, fmt.Errorf("invalid FUNCTION_DELIVERY_BACKOFF_POLICY %q: expected %q or %q", v, backoffExponential, backoffLinear)
		}
	}

	baseDelaySet := false
	if v, ok := lookupNonEmptyEnv("FUNCTION_DELIVERY_BACKOFF_DELAY"); ok {
		d, err := parseNonNegativeDuration("FUNCTION_DELIVERY_BACKOFF_DELAY", v)
		if err != nil {
			return deliveryConfig{}, err
		}
		c.baseDelay = d
		baseDelaySet = true
	}

	maxDelaySet := false
	if v, ok := lookupNonEmptyEnv("FUNCTION_DELIVERY_BACKOFF_MAX_DELAY"); ok {
		d, err := parseNonNegativeDuration("FUNCTION_DELIVERY_BACKOFF_MAX_DELAY", v)
		if err != nil {
			return deliveryConfig{}, err
		}
		c.maxDelay = d
		maxDelaySet = true
	}

	// A max below the base would silently clamp every delay to the base; reject
	// it so the configuration is unambiguous. Either value may come from a
	// default (the common case is setting only the base and tripping over the
	// 30s default max), so the message flags which side is a default rather than
	// pointing the user at a variable they never set.
	if c.maxDelay < c.baseDelay {
		return deliveryConfig{}, fmt.Errorf("%s must be >= %s",
			describeDelayValue("FUNCTION_DELIVERY_BACKOFF_MAX_DELAY", c.maxDelay, maxDelaySet),
			describeDelayValue("FUNCTION_DELIVERY_BACKOFF_DELAY", c.baseDelay, baseDelaySet))
	}

	return c, nil
}

// describeDelayValue renders a delay for the max/base validation error, noting
// when the value is the built-in default rather than one the user supplied, so
// the error never tells the user to raise or lower a variable they never set.
func describeDelayValue(envName string, d time.Duration, fromEnv bool) string {
	if fromEnv {
		return fmt.Sprintf("%s (%s)", envName, d)
	}
	return fmt.Sprintf("%s (default %s, unset)", envName, d)
}

// lookupNonEmptyEnv returns the trimmed value of an environment variable and
// whether it was set to a non-empty value. An unset or blank variable is
// reported as absent so it falls back to the default.
func lookupNonEmptyEnv(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	if !ok {
		return "", false
	}
	if v = strings.TrimSpace(v); v == "" {
		return "", false
	}
	return v, true
}

// parseNonNegativeDuration parses a Go duration and rejects negatives, with an
// error naming the offending variable.
func parseNonNegativeDuration(name, v string) (time.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, v, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid %s %q: must not be negative", name, v)
	}
	return d, nil
}
