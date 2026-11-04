// Package hotel implements the typed benchmark adapter for DeathStarBench's
// Hotel Reservation application.
package hotel

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

const (
	operationSearch            = "search_hotels"
	operationRecommendDistance = "recommend_distance"
	operationRecommendRate     = "recommend_rate"
	operationRecommendPrice    = "recommend_price"
	operationLoginValid        = "login_valid"
	operationLoginInvalid      = "login_invalid"
	operationReserveCapacity   = "reserve_capacity"
)

var knownOperations = map[string]struct{}{
	operationSearch:            {},
	operationRecommendDistance: {},
	operationRecommendRate:     {},
	operationRecommendPrice:    {},
	operationLoginValid:        {},
	operationLoginInvalid:      {},
	operationReserveCapacity:   {},
}

type Config = hotelsupport.Config

type Application struct {
	config Config
	seed   int64

	// reference computes expected answers from the task's dataset; the Zipf
	// samplers draw cities, users and hotels within a city by rank.
	reference  *hotelsupport.Reference
	cities     *dataset.Zipf
	users      *dataset.Zipf
	cityHotels []*dataset.Zipf

	// A reservation plan holds this lock until FinishOperation. The service's
	// capacity check and write are not transactional, so concurrent fill plans
	// could both observe spare capacity and make the rejection oracle ambiguous.
	reservationMu sync.Mutex
}

type fixture struct {
	dateBase        time.Time
	nextReservation atomic.Uint64

	// night and used (guarded by Application.reservationMu) are the current
	// reservation night and the hotels already reserved on it.
	night uint64
	used  map[int]struct{}
}

func (f *fixture) isUsed(hotel int) bool {
	_, used := f.used[hotel]
	return used
}

const (
	// Each trial owns a block of reservationBlockDays nights in the far
	// future, and books up to reservationsPerNight distinct hotels per night.
	reservationBlockDays = 512
	reservationsPerNight = 128
	reservationSlots     = reservationBlockDays * reservationsPerNight
	// reservationCycles 400-year Gregorian cycles from 3000-01-01 hold the
	// blocks (years 3000 through 9799, still four-digit dates).
	reservationCycles = 17
)

var _ api.Application = (*Application)(nil)
var _ api.PreflightApplication = (*Application)(nil)

func New(workload api.Workload) (api.Application, error) {
	config, err := hotelsupport.ValidateTopology(workload)
	if err != nil {
		return nil, err
	}
	for _, operation := range workload.Operations {
		if _, ok := knownOperations[operation.Name]; !ok {
			return nil, fmt.Errorf("unknown Hotel operation %q", operation.Name)
		}
		if operation.Target != hotelsupport.GatewayTarget {
			return nil, fmt.Errorf("Hotel operation %q must target %q", operation.Name, hotelsupport.GatewayTarget)
		}
		if operation.HTTP != nil {
			return nil, fmt.Errorf("Hotel operation %q must not declare operations.http", operation.Name)
		}
	}
	reference := hotelsupport.DefaultReference()
	application := &Application{
		config: config, seed: workload.Load.Seed, reference: reference,
		cities: dataset.NewZipf(len(reference.Data.Cities), dataset.ZipfOne),
		users:  dataset.NewZipf(len(reference.Data.Users), dataset.ZipfThreeQuarters),
	}
	for _, city := range reference.Data.Cities {
		application.cityHotels = append(application.cityHotels, dataset.NewZipf(len(city.Hotels), dataset.ZipfThreeQuarters))
	}
	return application, nil
}

func (a *Application) Name() string { return "hotel" }

func (a *Application) ReadinessProbes() []api.ReadinessProbe {
	return hotelsupport.ReadinessProbes(a.seed)
}

func (a *Application) PreflightProbes() []api.ReadinessProbe {
	return hotelsupport.PreflightProbes()
}

func (a *Application) Prepare(_ context.Context, _ api.Runtime, trial api.TrialContext) (any, error) {
	// Reserve only far-future nights, outside the rate window that searches
	// use, and give every trial its own block of them without setup-only
	// traffic. The fixture seed's digest places the run's first block; trials
	// of one run take consecutive blocks, so they never share a night, and
	// runs with different fixture seeds (the phases of one job) collide only
	// if their digests land within a few blocks of each other (about 1 in
	// 1000 for two phases of 2 and 3 trials).
	digest := sha256.Sum256([]byte(fmt.Sprintf("hotel-reservations/%d", trial.FixtureSeed)))
	rangeStart := time.Date(3000, time.January, 1, 12, 0, 0, 0, time.UTC)
	const blocks = reservationCycles * 146097 / reservationBlockDays
	block := (binary.BigEndian.Uint64(digest[:8]) + uint64(trial.Index)) % blocks
	return &fixture{
		dateBase: rangeStart.AddDate(0, 0, int(block)*reservationBlockDays),
	}, nil
}

func (a *Application) Reset(context.Context, api.Runtime, api.TrialContext) error {
	// DeathStarBench exposes no reservation deletion API. The fixture therefore
	// gives every trial a separate namespace and every scheduled sample
	// receives a unique date within that trial.
	return nil
}
