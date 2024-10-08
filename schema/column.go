package schema

import "github.com/pluto-metrics/rowbinary"

type column struct {
	Name string
	Type rowbinary.Any
}
