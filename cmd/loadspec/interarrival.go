package loadspec

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	loadDefSep     = ":"
	constLoadDef   = "const"
	poissonLoadDef = "poisson"
)

type interArrival interface {
	Next() int64
}

func newInterArrival(def string) (interArrival, error) {
	p := strings.Split(def, loadDefSep)
	if len(p) < 1 {
		return nil, fmt.Errorf("invalid inter arrival definition:%s", def)
	}
	switch p[0] {
	case constLoadDef:
		qps, err := strconv.ParseFloat(p[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid inter arrival definition:%q", err)
		}
		return &Const{qps}, nil
	case poissonLoadDef:
		lambda, err := strconv.ParseFloat(p[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid inter arrival definition:%q", err)
		}
		return &Poisson{lambda}, nil
	default:
		return nil, fmt.Errorf("invalid load type:%s", p[0])
	}
}

// Generates a stream of inter-arrival times following a constant number of queries per second.
type Const struct {
	qps float64
}

func (g *Const) Next() int64 {
	return int64(float64(1e9) / g.qps)
}

// Generates an sequence of inter-arrival times following the Poisson distribution.
type Poisson struct {
	// The rate parameter λ is a measure of frequency: the average rate of events (in this case, messages sent)
	// per unit of time (in this case, seconds).
	lambda float64
}

func (p *Poisson) Next() int64 {
	// NOTE: Implementation follows:
	// http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
	return int64(-math.Log(1.0-randGen.Float64()) / float64(p.lambda) * float64(1e9))
}
