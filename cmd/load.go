package cmd

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	loadDefSep     = ":"
	constLoadDef   = "const"
	poissonLoadDef = "poisson"
)

type LoadGen interface {
	GetTicker() <-chan struct{}
	Start() LoadGen
}

type ConstLoadGen struct {
	Qps float64
	c   chan struct{}
}

func (g *ConstLoadGen) GetTicker() <-chan struct{} {
	if g.c == nil {
		g.c = make(chan struct{})
	}
	return g.c
}

func (g *ConstLoadGen) Start() LoadGen {
	go func() {
		t := time.Duration((float64(1) / g.Qps) * 1e9)
		for {
			// Non-blocking ticking on c.
			select {
			case g.c <- struct{}{}:
			default:
			}
			time.Sleep(t)
		}
	}()
	return g
}

func NewConstLoadGen(params []string) (LoadGen, error) {
	qps, err := strconv.ParseFloat(params[0], 64)
	if err != nil {
		return nil, err
	}
	return &ConstLoadGen{Qps: qps}, nil
}

type PoissonLoadGen struct {
	// The rate parameter λ is a measure of frequency: the average rate of events (in this case, messages sent)
	// per unit of time (in this case, seconds).
	Lambda float64
	c      chan struct{}
}

func (g *PoissonLoadGen) GetTicker() <-chan struct{} {
	if g.c == nil {
		g.c = make(chan struct{})
	}
	return g.c
}

func (g *PoissonLoadGen) Start() LoadGen {
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			// Non-blocking ticking on c.
			// NOTE: Implementation follows:
			// http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
			select {
			case g.c <- struct{}{}:
			default:
			}

			time.Sleep(time.Duration((-math.Log(1.0-r.Float64()) / float64(g.Lambda)) * 1e9))
		}
	}()
	return g
}

func NewPoissonLoadGen(params []string) (LoadGen, error) {
	qps, err := strconv.ParseFloat(params[0], 64)
	if err != nil {
		return nil, err
	}
	return &PoissonLoadGen{Lambda: qps}, nil
}

func NewLoadGen(def string) (LoadGen, error) {
	p := strings.Split(def, loadDefSep)
	if len(p) < 1 {
		return nil, fmt.Errorf("Invalid load definition:%s", def)
	}
	switch p[0] {
	case constLoadDef:
		return NewConstLoadGen(p[1:])
	case poissonLoadDef:
		return NewPoissonLoadGen(p[1:])
	default:
		return nil, fmt.Errorf("Invalid load type:%s", p[0])
	}
}
