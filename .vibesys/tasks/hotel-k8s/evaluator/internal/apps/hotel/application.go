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
	operationRates:             {},
	operationUpdateRate:        {},
	operationUpdateProfile:     {},
	operationSetCapacity:       {},
	operationAddHotel:          {},
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

	// writes records the admin writes of every trial (they outlive a trial
	// until Reset restores them).
	writes *writeModel
}

type fixture struct {
	dateBase        time.Time
	nextReservation atomic.Uint64

	// night and used (guarded by Application.reservationMu) are the current
	// reservation night and the hotels already reserved on it.
	night uint64
	used  map[int]struct{}

	// sandbox is the trial's cell of the additions' region and tag marks its
	// hotel IDs; added (guarded by writeModel.mu) are its additions.
	sandbox, tag uint64
	added        []*addition
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
	reservationBlocks = reservationCycles * 146097 / reservationBlockDays

	// TrialsPerRun bounds the trials (repetitions) of one load-generator
	// run. A run owns TrialsPerRun consecutive trial slots, and RunSlots runs
	// fit in the reservation blocks.
	TrialsPerRun = 16
	RunSlots     = reservationBlocks / TrialsPerRun
)

// TrialSlot is the trial's fixture namespace, in [0, RunSlots*TrialsPerRun):
// its reservation block, its additions' sandbox cell (a multiple of the
// slot), and the high bits of
// its added hotels' ID tag. The run's fixture seed F (the engine passes
// trial.FixtureSeed = F + trial.Index) selects run slot F mod RunSlots,
// and the trial index the slot within it. So the trials of one run never
// share a slot, and two runs whose fixture seeds differ by 1 to RunSlots-1
// (run.py gives the open phase the closed phase's seed plus 1) are disjoint
// by construction: nights, sandbox cells and hotel IDs.
func TrialSlot(trial api.TrialContext) (uint64, error) {
	if trial.Index < 0 || trial.Index >= TrialsPerRun {
		return 0, fmt.Errorf("Hotel runs at most %d trials, got trial index %d", TrialsPerRun, trial.Index)
	}
	run := trial.FixtureSeed - int64(trial.Index)
	slot := ((run % RunSlots) + RunSlots) % RunSlots
	return uint64(slot)*TrialsPerRun + uint64(trial.Index), nil
}

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
		config: config, seed: workload.Load.Seed, reference: reference, writes: newWriteModel(),
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
	// use, and add hotels only in the trial's own sandbox cell: the trial
	// slot (see TrialSlot) places both, without setup-only traffic.
	slot, err := TrialSlot(trial)
	if err != nil {
		return nil, err
	}
	rangeStart := time.Date(3000, time.January, 1, 12, 0, 0, 0, time.UTC)
	// The tag's low bits vary the added hotels' IDs between jobs; its high
	// bits (the slot) keep them distinct between the trials of a namespace.
	additions := sha256.Sum256([]byte(fmt.Sprintf("hotel-additions/%d/%d", trial.FixtureSeed, trial.Index)))
	// Cells spread the slots over the whole sandbox region.
	cellStride := dataset.SandboxCount() / (RunSlots * TrialsPerRun)
	return &fixture{
		dateBase: rangeStart.AddDate(0, 0, int(slot)*reservationBlockDays),
		sandbox:  slot * cellStride,
		tag:      slot<<32 | uint64(binary.BigEndian.Uint32(additions[:4])),
	}, nil
}
