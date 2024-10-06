package schema

import "github.com/pluto-metrics/rowbinary/types"

type column struct {
	Name string
	Type types.Any
}
