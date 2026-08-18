package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type reader struct {
	src    Source
	errors []error
}

func (r *reader) raw(key string) (string, bool) {
	v, ok := r.src(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func (r *reader) str(key, def string) string {
	if v, ok := r.raw(key); ok {
		return v
	}
	return def
}

func (r *reader) list(key string, def []string) []string {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func (r *reader) integer(key string, def int) int {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errors = append(r.errors, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errors = append(r.errors, fmt.Errorf("%s: %q is not a duration", key, v))
		return def
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errors = append(r.errors, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

func (r *reader) enum(key, def string, allowed ...string) string {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	v = strings.ToLower(v)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	r.errors = append(r.errors, fmt.Errorf("%s: %q is not one of %s", key, v, strings.Join(allowed, ", ")))
	return def
}

func (r *reader) err() error { return errors.Join(r.errors...) }

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
