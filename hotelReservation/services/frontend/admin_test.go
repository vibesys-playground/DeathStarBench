package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	geo "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	profile "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	rate "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	recommendation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	reservation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// calls records the downstream calls in order; fail makes one fail.
type calls struct {
	log  []string
	fail map[string]error
}

func (c *calls) call(name string) error {
	c.log = append(c.log, name)
	return c.fail[name]
}

type fakeRate struct {
	rate.RateClient
	*calls
	last *rate.UpdateRateRequest
}

func (f *fakeRate) GetRates(_ context.Context, in *rate.Request, _ ...grpc.CallOption) (*rate.Result, error) {
	return &rate.Result{RatePlans: []*rate.RatePlan{{
		HotelId: in.HotelIds[0], Code: "RACK", InDate: "2027-01-01", OutDate: "2028-01-01",
		RoomType: &rate.RoomType{Code: "KNG", RoomDescription: "King", BookableRate: 100, TotalRate: 100, TotalRateInclusive: 120},
	}}}, f.call("rate.GetRates")
}

func (f *fakeRate) UpdateRate(_ context.Context, in *rate.UpdateRateRequest, _ ...grpc.CallOption) (*rate.UpdateRateResult, error) {
	f.last = in
	return &rate.UpdateRateResult{}, f.call("rate.UpdateRate")
}

func (f *fakeRate) AddRates(_ context.Context, in *rate.AddRatesRequest, _ ...grpc.CallOption) (*rate.AddRatesResult, error) {
	return &rate.AddRatesResult{}, f.call("rate.AddRates")
}

type fakeProfile struct {
	profile.ProfileClient
	*calls
	added *profile.Hotel
}

func (f *fakeProfile) UpdateProfile(_ context.Context, in *profile.UpdateRequest, _ ...grpc.CallOption) (*profile.UpdateResult, error) {
	return &profile.UpdateResult{}, f.call("profile.UpdateProfile")
}

func (f *fakeProfile) AddProfile(_ context.Context, in *profile.Hotel, _ ...grpc.CallOption) (*profile.UpdateResult, error) {
	f.added = in
	return &profile.UpdateResult{}, f.call("profile.AddProfile")
}

type fakeReservation struct {
	reservation.ReservationClient
	*calls
}

func (f *fakeReservation) SetCapacity(_ context.Context, in *reservation.CapacityRequest, _ ...grpc.CallOption) (*reservation.CapacityResult, error) {
	return &reservation.CapacityResult{}, f.call("reservation.SetCapacity")
}

func (f *fakeReservation) AddCapacity(_ context.Context, in *reservation.CapacityRequest, _ ...grpc.CallOption) (*reservation.CapacityResult, error) {
	return &reservation.CapacityResult{}, f.call("reservation.AddCapacity")
}

type fakeRecommendation struct {
	recommendation.RecommendationClient
	*calls
}

func (f *fakeRecommendation) AddHotel(_ context.Context, in *recommendation.Hotel, _ ...grpc.CallOption) (*recommendation.AddResult, error) {
	return &recommendation.AddResult{}, f.call("recommendation.AddHotel")
}

type fakeGeo struct {
	geo.GeoClient
	*calls
}

func (f *fakeGeo) AddHotel(_ context.Context, in *geo.Point, _ ...grpc.CallOption) (*geo.AddResult, error) {
	return &geo.AddResult{}, f.call("geo.AddHotel")
}

func newFakeServer() (*Server, *calls, *fakeRate, *fakeProfile) {
	c := &calls{fail: map[string]error{}}
	r, p := &fakeRate{calls: c}, &fakeProfile{calls: c}
	return &Server{
		rateClient: r, profileClient: p, reservationClient: &fakeReservation{calls: c},
		recommendationClient: &fakeRecommendation{calls: c}, geoClient: &fakeGeo{calls: c},
	}, c, r, p
}

func serve(handler http.HandlerFunc, method, target, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(method, target, strings.NewReader(body)))
	return recorder
}

func TestAdminRateUpdate(t *testing.T) {
	server, c, r, _ := newFakeServer()
	response := serve(server.adminRateHandler, http.MethodPost,
		"/admin/rate?hotelId=7&code=RACK&bookableRate=101.5&totalRate=101.5&totalRateInclusive=121.8", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Rate updated.") {
		t.Fatalf("got %d %q", response.Code, response.Body.String())
	}
	if r.last.HotelId != "7" || r.last.Code != "RACK" || r.last.TotalRate != 101.5 || r.last.TotalRateInclusive != 121.8 {
		t.Fatalf("request %+v", r.last)
	}
	for _, target := range []string{
		"/admin/rate?hotelId=7&bookableRate=1&totalRate=1&totalRateInclusive=1",
		"/admin/rate?hotelId=7&code=RACK&bookableRate=-1&totalRate=1&totalRateInclusive=1",
		"/admin/rate?hotelId=7&code=RACK&bookableRate=NaN&totalRate=1&totalRateInclusive=1",
		"/admin/rate?hotelId=7&code=RACK&totalRate=1&totalRateInclusive=1",
	} {
		if response := serve(server.adminRateHandler, http.MethodPost, target, ""); response.Code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", target, response.Code)
		}
	}
	if response := serve(server.adminRateHandler, http.MethodGet, "/admin/rate?hotelId=7", ""); response.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d, want 405", response.Code)
	}
	c.fail["rate.UpdateRate"] = status.Error(codes.NotFound, "no plan")
	if response := serve(server.adminRateHandler, http.MethodPost,
		"/admin/rate?hotelId=7&code=X&bookableRate=1&totalRate=1&totalRateInclusive=1", ""); response.Code != http.StatusNotFound {
		t.Errorf("unknown plan: %d, want 404", response.Code)
	}
}

func TestAdminProfileAndCapacityStatuses(t *testing.T) {
	server, c, _, _ := newFakeServer()
	if response := serve(server.adminProfileHandler, http.MethodPost, "/admin/profile?hotelId=7&phone=%2B44+1", ""); response.Code != http.StatusOK {
		t.Fatalf("profile update: %d", response.Code)
	}
	if response := serve(server.adminProfileHandler, http.MethodPost, "/admin/profile?hotelId=7", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("profile update without fields: %d", response.Code)
	}
	// A form body works like the query string.
	form := httptest.NewRequest(http.MethodPost, "/admin/capacity", strings.NewReader("hotelId=7&rooms=12"))
	form.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	server.adminCapacityHandler(recorder, form)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "Capacity updated.") {
		t.Fatalf("capacity update: %d %q", recorder.Code, recorder.Body.String())
	}
	for _, target := range []string{"/admin/capacity?hotelId=7&rooms=-1", "/admin/capacity?hotelId=7&rooms=x", "/admin/capacity?rooms=3"} {
		if response := serve(server.adminCapacityHandler, http.MethodPost, target, ""); response.Code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", target, response.Code)
		}
	}
	c.fail["reservation.SetCapacity"] = status.Error(codes.FailedPrecondition, "reserved")
	if response := serve(server.adminCapacityHandler, http.MethodPost, "/admin/capacity?hotelId=7&rooms=1", ""); response.Code != http.StatusConflict {
		t.Errorf("capacity below reservations: %d, want 409", response.Code)
	}
}

const newHotelBody = `{"hotelId":"a1-2","name":"Hotel","phoneNumber":"+1 2","description":"d",
"address":{"streetNumber":"1","streetName":"Main St","city":"C","state":"S","country":"X","postalCode":"0"},
"lat":-40.5,"lon":-120.25,"rating":4.1,"price":99.5,"rooms":40,
"ratePlans":[{"code":"RACK","inDate":"2027-01-01","outDate":"2028-01-01",
"roomType":{"code":"KNG","roomDescription":"King","bookableRate":99.5,"totalRate":99.5,"totalRateInclusive":110}}]}`

func TestAdminHotelStoresOwnersBeforeTheIndexes(t *testing.T) {
	server, c, _, p := newFakeServer()
	response := serve(server.adminHotelHandler, http.MethodPost, "/admin/hotel", newHotelBody)
	if response.Code != http.StatusOK {
		t.Fatalf("add: %d %q", response.Code, response.Body.String())
	}
	want := "profile.AddProfile,rate.AddRates,reservation.AddCapacity,recommendation.AddHotel,geo.AddHotel"
	if got := strings.Join(c.log, ","); got != want {
		t.Fatalf("calls %s, want %s", got, want)
	}
	if p.added.Address.Lat != float32(-40.5) || p.added.Address.City != "C" {
		t.Fatalf("profile %+v", p.added)
	}

	c.log = nil
	c.fail["profile.AddProfile"] = status.Error(codes.AlreadyExists, "exists")
	if response := serve(server.adminHotelHandler, http.MethodPost, "/admin/hotel", newHotelBody); response.Code != http.StatusConflict {
		t.Fatalf("taken ID: %d, want 409", response.Code)
	}
	if len(c.log) != 1 {
		t.Fatalf("a rejected addition reached %v", c.log)
	}
	for name, body := range map[string]string{
		"underscore in ID": strings.Replace(newHotelBody, `"a1-2"`, `"a1_2"`, 1),
		"no plans":         newHotelBody[:strings.Index(newHotelBody, `"ratePlans"`)] + `"ratePlans":[]}`,
		"bad latitude":     strings.Replace(newHotelBody, `-40.5`, `-91`, 1),
		"unknown field":    strings.Replace(newHotelBody, `"rooms":40`, `"rooms":40,"x":1`, 1),
		"not JSON":         "hotelId=1",
	} {
		if response := serve(server.adminHotelHandler, http.MethodPost, "/admin/hotel", body); response.Code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", name, response.Code)
		}
	}
}

func TestRatesRoute(t *testing.T) {
	server, _, _, _ := newFakeServer()
	response := serve(server.ratesHandler, http.MethodGet, "/rates?hotelId=9", "")
	var body struct {
		HotelId   string `json:"hotelId"`
		RatePlans []struct {
			Code     string         `json:"code"`
			RoomType map[string]any `json:"roomType"`
		} `json:"ratePlans"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != http.StatusOK {
		t.Fatalf("%d %q %v", response.Code, response.Body.String(), err)
	}
	if body.HotelId != "9" || len(body.RatePlans) != 1 || body.RatePlans[0].RoomType["totalRateInclusive"] != 120.0 {
		t.Fatalf("body %+v", body)
	}
	if response := serve(server.ratesHandler, http.MethodGet, "/rates", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("missing hotelId: %d", response.Code)
	}
}
