package rowbinary

import (
	"testing"
	"time"
)

func TestTimeZoneTypesShareLoadedLocation(t *testing.T) {
	a := DateTimeTZ("Asia/Shanghai").(*typeWrapper[time.Time]).PreType.(typeWrapperAny[time.Time]).BaseType.(typeDateTimeTZ)
	b := DateTimeTZ("Asia/Shanghai").(*typeWrapper[time.Time]).PreType.(typeWrapperAny[time.Time]).BaseType.(typeDateTimeTZ)
	c := DateTime64TZ(3, "Asia/Shanghai").(*typeWrapper[time.Time]).PreType.(typeWrapperAny[time.Time]).BaseType.(typeDateTime64TZ)
	if a.loc == nil || a.loc != b.loc || a.loc != c.loc {
		t.Fatalf("locations not shared: %p %p %p", a.loc, b.loc, c.loc)
	}
	if _, err := loadLocation("No/Such_Zone"); err == nil {
		t.Fatal("unknown zone accepted")
	}
}
