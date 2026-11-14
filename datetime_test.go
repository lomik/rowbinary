package rowbinary

import (
	"bytes"
	"testing"
	"time"
)

func TestDateTimeLocationIndependent(t *testing.T) {
	for _, tp := range []Type[time.Time]{DateTime, DateTimeTZ("UTC")} {
		t.Run(tp.String(), func(t *testing.T) {
			instant := time.Unix(3600, 0)
			var utc, local bytes.Buffer
			if err := tp.Write(NewWriter(&utc), instant.UTC()); err != nil {
				t.Fatal(err)
			}
			if err := tp.Write(NewWriter(&local), instant.In(time.FixedZone("UTC-8", -8*3600))); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(utc.Bytes(), local.Bytes()) {
				t.Fatalf("same instant: UTC wire %x, local wire %x", utc.Bytes(), local.Bytes())
			}
		})
	}
}
