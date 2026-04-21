package consume

import (
	"math"
	"testing"
	"time"
)

// testParseOffset creates a consumption with the given offset string,
// calls parseOffset, and returns the consumption so callers can inspect
// the side-effect fields.
func testParseOffset(t *testing.T, offset string) *consumption {
	t.Helper()
	c := &consumption{offset: offset}
	_, err := c.parseOffset()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseOffset_Start(t *testing.T) {
	c := testParseOffset(t, "start")

	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
	if c.startTimestampMillis != -1 {
		t.Errorf("startTimestampMillis: got %d, want -1", c.startTimestampMillis)
	}
	if c.endTimestampMillis != -1 {
		t.Errorf("endTimestampMillis: got %d, want -1", c.endTimestampMillis)
	}
}

func TestParseOffset_End(t *testing.T) {
	c := testParseOffset(t, "end")

	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
	if c.startTimestampMillis != -1 {
		t.Errorf("startTimestampMillis: got %d, want -1", c.startTimestampMillis)
	}
	if c.endTimestampMillis != -1 {
		t.Errorf("endTimestampMillis: got %d, want -1", c.endTimestampMillis)
	}
}

func TestParseOffset_StartPlus5(t *testing.T) {
	c := testParseOffset(t, "start+5")

	// start+5 is KindStart with Delta=5; no exact offset set.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_EndMinus3(t *testing.T) {
	c := testParseOffset(t, "end-3")

	// end-3 is KindEnd with Delta=-3; no side-effect fields set.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_RelativePlus10(t *testing.T) {
	c := testParseOffset(t, "+10")

	// +10 is KindRelative with Value=10; for consume, interpreted as
	// AtStart().Relative(10). No side-effect fields set.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_RelativeMinus5(t *testing.T) {
	c := testParseOffset(t, "-5")

	// -5 is KindRelative with Value=-5; for consume, interpreted as
	// AtEnd().Relative(-5). No side-effect fields set.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_Exact100(t *testing.T) {
	c := testParseOffset(t, "100")

	if c.start != 100 {
		t.Errorf("start: got %d, want 100", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_ExactRange100to200(t *testing.T) {
	c := testParseOffset(t, "100:200")

	if c.start != 100 {
		t.Errorf("start: got %d, want 100", c.start)
	}
	if c.end != 200 {
		t.Errorf("end: got %d, want 200", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_UntilEnd(t *testing.T) {
	c := testParseOffset(t, ":end")

	// :end means consume from start until end (high watermark).
	// untilOffset=0, addUntilOffset=false.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
	if c.untilOffset != 0 {
		t.Errorf("untilOffset: got %d, want 0", c.untilOffset)
	}
	if c.addUntilOffset {
		t.Errorf("addUntilOffset: got true, want false")
	}
}

func TestParseOffset_UntilEndPlus3(t *testing.T) {
	c := testParseOffset(t, ":end+3")

	// :end+3 means consume until end+3 (high watermark + 3).
	if c.untilOffset != 3 {
		t.Errorf("untilOffset: got %d, want 3", c.untilOffset)
	}
	if !c.addUntilOffset {
		t.Errorf("addUntilOffset: got false, want true")
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
}

func TestParseOffset_UntilEndMinus2(t *testing.T) {
	c := testParseOffset(t, ":end-2")

	// :end-2 means consume until end-2 (high watermark - 2).
	if c.untilOffset != 2 {
		t.Errorf("untilOffset: got %d, want 2", c.untilOffset)
	}
	if c.addUntilOffset {
		t.Errorf("addUntilOffset: got true, want false")
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
}

func TestParseOffset_UntilExact500(t *testing.T) {
	c := testParseOffset(t, ":500")

	// :500 means consume from start until exact offset 500.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != 500 {
		t.Errorf("end: got %d, want 500", c.end)
	}
	if c.untilOffset != -1 {
		t.Errorf("untilOffset: got %d, want -1", c.untilOffset)
	}
}

func TestParseOffset_TimestampExact(t *testing.T) {
	c := testParseOffset(t, "@1700000000000")

	if c.startTimestampMillis != 1700000000000 {
		t.Errorf("startTimestampMillis: got %d, want 1700000000000", c.startTimestampMillis)
	}
	if c.endTimestampMillis != -1 {
		t.Errorf("endTimestampMillis: got %d, want -1", c.endTimestampMillis)
	}
	// Exact offset fields should not be set for timestamp-based.
	if c.start != 0 {
		t.Errorf("start: got %d, want 0", c.start)
	}
	if c.end != -1 {
		t.Errorf("end: got %d, want -1", c.end)
	}
}

func TestParseOffset_TimestampRelativeDuration(t *testing.T) {
	before := time.Now()
	c := testParseOffset(t, "@-1h")
	after := time.Now()

	if c.startTimestampMillis < 0 {
		t.Fatalf("startTimestampMillis should be >= 0, got %d", c.startTimestampMillis)
	}

	// The resolved timestamp should be approximately now - 1 hour.
	expectedLow := before.Add(-1 * time.Hour).UnixMilli()
	expectedHigh := after.Add(-1 * time.Hour).UnixMilli()

	if c.startTimestampMillis < expectedLow || c.startTimestampMillis > expectedHigh {
		t.Errorf("startTimestampMillis: got %d, want between %d and %d", c.startTimestampMillis, expectedLow, expectedHigh)
	}

	if c.endTimestampMillis != -1 {
		t.Errorf("endTimestampMillis: got %d, want -1", c.endTimestampMillis)
	}

	// Verify it is approximately 1 hour ago (within 5 seconds tolerance).
	nowMillis := time.Now().UnixMilli()
	oneHourMillis := int64(time.Hour / time.Millisecond)
	diff := math.Abs(float64(nowMillis - c.startTimestampMillis - oneHourMillis))
	if diff > 5000 {
		t.Errorf("startTimestampMillis is not approximately 1 hour ago: diff from expected = %.0f ms", diff)
	}
}
