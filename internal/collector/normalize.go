package collector

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	narkv1 "github.com/alfacrab/nark/gen/go/nark/v1"
)

// Field limits applied to every accepted track. They protect ClickHouse from
// unbounded cardinality and keep a single record small enough to batch well.
const (
	maxIDLen         = 64
	maxNameLen       = 128
	maxKeyLen        = 128
	maxValueLen      = 1024
	maxURLLen        = 2048
	maxTitleLen      = 512
	maxAttributes    = 64
	maxMetrics       = 64
	maxUserAgentLen  = 512
	maxBacklogWindow = 7 * 24 * time.Hour
)

// Low-cardinality rejection reasons, used both as metric labels and as the
// machine readable part of the per-track error returned to the caller.
const (
	reasonEmptyTrack     = "empty_track"
	reasonMissingName    = "missing_name"
	reasonMissingKind    = "missing_kind"
	reasonTooManyFields  = "too_many_fields"
	reasonInvalidNumber  = "invalid_number"
	reasonTooOld         = "occurred_at_too_old"
	reasonMissingSubject = "missing_subject"
)

// rejection is a per-track validation failure. The batch containing it is still
// accepted; only the offending track is dropped and reported back.
type rejection struct {
	reason string
	detail string
}

func (r rejection) Error() string { return r.detail }

func reject(reason, format string, args ...any) rejection {
	return rejection{reason: reason, detail: fmt.Sprintf(format, args...)}
}

// normalizer validates and rewrites tracks in place. It is created once per
// service and is safe for concurrent use as long as its funcs are.
type normalizer struct {
	now          func() time.Time
	newID        func() string
	maxClockSkew time.Duration
}

// normalize enforces the limits above and fills in server-side defaults. It
// reports whether occurred_at had to be corrected for client clock skew.
func (n *normalizer) normalize(t *narkv1.Track) (skewCorrected bool, err error) {
	if t == nil {
		return false, reject(reasonEmptyTrack, "track is nil")
	}

	t.Name = strings.TrimSpace(t.GetName())

	if t.GetName() == "" {
		return false, reject(reasonMissingName, "name is required")
	}

	t.Name = truncate(t.GetName(), maxNameLen)

	if t.GetKind() == narkv1.TrackKind_TRACK_KIND_UNSPECIFIED {
		return false, reject(reasonMissingKind, "kind is required")
	}

	if len(t.GetAttributes()) > maxAttributes {
		return false, reject(reasonTooManyFields, "at most %d attributes are allowed, got %d", maxAttributes, len(t.GetAttributes()))
	}

	if len(t.GetMetrics()) > maxMetrics {
		return false, reject(reasonTooManyFields, "at most %d metrics are allowed, got %d", maxMetrics, len(t.GetMetrics()))
	}

	if !isFinite(t.GetValue()) {
		return false, reject(reasonInvalidNumber, "value must be a finite number")
	}

	for k, v := range t.GetMetrics() {
		if !isFinite(v) {
			return false, reject(reasonInvalidNumber, "metric %q must be a finite number", k)
		}
	}

	t.Id = truncate(strings.TrimSpace(t.GetId()), maxIDLen)

	if t.GetId() == "" {
		t.Id = n.newID()
	}

	t.UserId = truncate(strings.TrimSpace(t.GetUserId()), maxIDLen)
	t.SessionId = truncate(strings.TrimSpace(t.GetSessionId()), maxIDLen)
	t.DeviceId = truncate(strings.TrimSpace(t.GetDeviceId()), maxIDLen)

	if t.GetUserId() == "" && t.GetSessionId() == "" && t.GetDeviceId() == "" {
		return false, reject(reasonMissingSubject, "one of user_id, session_id or device_id is required")
	}

	t.Currency = truncate(strings.ToUpper(strings.TrimSpace(t.GetCurrency())), 3)
	normalizePage(t.GetPage())
	t.Attributes = normalizeStringMap(t.GetAttributes())
	t.Metrics = normalizeMetricKeys(t.GetMetrics())

	now := n.now()
	if !hasTimestamp(t.GetOccurredAt()) {
		t.OccurredAt = timestamppb.New(now)
		return false, nil
	}

	switch occurred := t.GetOccurredAt().AsTime(); {
	case occurred.After(now.Add(n.maxClockSkew)):
		t.OccurredAt = timestamppb.New(now) // Clients with a skewed clock would otherwise land in future partitions.
		skewCorrected = true
	case occurred.Before(now.Add(-maxBacklogWindow)):
		return false, reject(reasonTooOld, "occurred_at is older than %s", maxBacklogWindow)
	}

	return skewCorrected, nil
}

// normalizeClient trims and truncates the batch-level client description.
func normalizeClient(c *narkv1.Client) *narkv1.Client {
	if c == nil {
		return nil
	}
	c.App = truncate(strings.TrimSpace(c.GetApp()), maxNameLen)
	c.AppVersion = truncate(strings.TrimSpace(c.GetAppVersion()), maxNameLen)
	c.SdkVersion = truncate(strings.TrimSpace(c.GetSdkVersion()), maxNameLen)
	c.Platform = truncate(strings.TrimSpace(c.GetPlatform()), maxNameLen)
	c.Locale = truncate(strings.TrimSpace(c.GetLocale()), 32)
	c.Timezone = truncate(strings.TrimSpace(c.GetTimezone()), 64)
	c.UserAgent = truncate(strings.TrimSpace(c.GetUserAgent()), maxUserAgentLen)
	return c
}

func normalizePage(p *narkv1.Page) {
	if p == nil {
		return
	}

	p.Url = truncate(strings.TrimSpace(p.GetUrl()), maxURLLen)
	p.Path = truncate(strings.TrimSpace(p.GetPath()), maxURLLen)
	p.Referrer = truncate(strings.TrimSpace(p.GetReferrer()), maxURLLen)
	p.Title = truncate(strings.TrimSpace(p.GetTitle()), maxTitleLen)
}

func normalizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))

	for k, v := range in {
		key := truncate(strings.TrimSpace(k), maxKeyLen)
		if key == "" {
			continue
		}
		out[key] = truncate(v, maxValueLen)
	}

	return out
}

func normalizeMetricKeys(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]float64, len(in))

	for k, v := range in {
		key := truncate(strings.TrimSpace(k), maxKeyLen)
		if key == "" {
			continue
		}
		out[key] = v
	}

	return out
}

// truncate cuts s to at most limit runes without splitting a UTF-8 sequence.
func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}

	if utf8.RuneCountInString(s) <= limit {
		return s
	}

	count := 0

	for i := range s {
		if count == limit {
			return s[:i]
		}
		count++
	}

	return s
}

func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// hasTimestamp reports whether the client actually set occurred_at. A nil or
// zero-value timestamp means "use ingest time".
func hasTimestamp(ts *timestamppb.Timestamp) bool {
	return ts.IsValid() && (ts.GetSeconds() != 0 || ts.GetNanos() != 0)
}
