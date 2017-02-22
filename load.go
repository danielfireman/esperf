package main

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
}

type ConstLoadGen struct {
	Interval time.Duration
}

func (g *ConstLoadGen) GetTicker() <-chan struct{} {
	c := make(chan struct{})
	go func() {
		for {
			c <- struct{}{}
			time.Sleep(g.Interval)
		}
	}()
	return c
}

func NewConstLoadGen(params []string) (LoadGen, error) {
	qps, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}
	req := qps / *workers // Number of requests per worker per second.
	if req == 0 {
		return nil, fmt.Errorf("To few requests per worker, please increase the qps or decrease the number of workers")
	}
	return &ConstLoadGen{time.Duration(time.Second.Nanoseconds() / int64(req))}, nil
}

type PoissonLoadGen struct {
	// The rate parameter λ is a measure of frequency: the average rate of events (in this case, messages sent)
	// per unit of time (in this case, seconds).
	Lambda float64
}

func (g *PoissonLoadGen) GetTicker() <-chan struct{} {
	c := make(chan struct{})
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			// NOTE: Implementation follows:
			// http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
			c <- struct{}{}
			time.Sleep(time.Duration(-math.Log(1.0-r.Float64())/g.Lambda) * time.Second)
		}
	}()
	return c
}

func NewPoissonLoadTicker(params []string) (LoadGen, error) {
	qps, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}
	// TODO(danielfireman): Make workers a parameter.
	numReq := (qps * int((*duration).Seconds())) / *workers
	if numReq == 0 {
		return nil, fmt.Errorf("To few requests per worker, please increase the qps or decrease the number of workers")
	}
	return &PoissonLoadGen{float64(numReq) / (*duration).Seconds()}, nil
}

func ParseLoadDef(def string) (LoadGen, error) {
	p := strings.Split(def, loadDefSep)
	if len(p) < 1 {
		return nil, fmt.Errorf("Invalid load definition:%s", def)
	}
	switch p[0] {
	case constLoadDef:
		return NewConstLoadGen(p[1:])
	case poissonLoadDef:
		return NewPoissonLoadTicker(p[1:])
	default:
		return nil, fmt.Errorf("Invalid load type:%s", p[0])
	}
}
