package schema

import "github.com/pluto-metrics/rowbinary/types"

type Column struct {
	Name string
	Type types.Any
}

func C(name string, tp types.Any) Column {
	return Column{Name: name, Type: tp}
}
