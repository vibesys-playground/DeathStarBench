package frontend

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"

	geo "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	profile "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	rate "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	recommendation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	reservation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The owner-side routes. Each write is acknowledged with HTTP 200 once the
// owning services stored it in MongoDB and invalidated their caches; see
// the task's OBJECTIVE.md for the visibility contract. Parameters come from
// the query string or a form body; /admin/hotel takes a JSON body.

// ratesHandler serves GET /rates?hotelId=: the hotel's rate plans.
func (s *Server) ratesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}
	rates, err := s.rateClient.GetRates(r.Context(), &rate.Request{HotelIds: []string{hotelId}})
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	plans := make([]interface{}, 0, len(rates.RatePlans))
	for _, plan := range rates.RatePlans {
		roomType := plan.RoomType
		if roomType == nil {
			roomType = &rate.RoomType{}
		}
		plans = append(plans, map[string]interface{}{
			"code":    plan.Code,
			"inDate":  plan.InDate,
			"outDate": plan.OutDate,
			"roomType": map[string]interface{}{
				"code":               roomType.Code,
				"roomDescription":    roomType.RoomDescription,
				"bookableRate":       roomType.BookableRate,
				"totalRate":          roomType.TotalRate,
				"totalRateInclusive": roomType.TotalRateInclusive,
			},
		})
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"hotelId": hotelId, "ratePlans": plans})
}

// adminRateHandler serves POST /admin/rate?hotelId=&code=&bookableRate=&totalRate=&totalRateInclusive=.
func (s *Server) adminRateHandler(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	hotelId, code := r.FormValue("hotelId"), r.FormValue("code")
	if hotelId == "" || code == "" {
		http.Error(w, "Please specify hotelId and code params", http.StatusBadRequest)
		return
	}
	var prices [3]float64
	for index, name := range []string{"bookableRate", "totalRate", "totalRateInclusive"} {
		value, ok := parseAmount(r.FormValue(name))
		if !ok {
			http.Error(w, "Please specify a non-negative "+name+" param", http.StatusBadRequest)
			return
		}
		prices[index] = value
	}
	_, err := s.rateClient.UpdateRate(r.Context(), &rate.UpdateRateRequest{
		HotelId: hotelId, Code: code,
		BookableRate: prices[0], TotalRate: prices[1], TotalRateInclusive: prices[2],
	})
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	writeMessage(w, "Rate updated.")
}

// adminProfileHandler serves POST /admin/profile?hotelId=&name=&phone=
// (name and phone optional, at least one).
func (s *Server) adminProfileHandler(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	hotelId, name, phone := r.FormValue("hotelId"), r.FormValue("name"), r.FormValue("phone")
	if hotelId == "" || (name == "" && phone == "") {
		http.Error(w, "Please specify hotelId and name or phone params", http.StatusBadRequest)
		return
	}
	_, err := s.profileClient.UpdateProfile(r.Context(), &profile.UpdateRequest{
		HotelId: hotelId, Name: name, PhoneNumber: phone,
	})
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	writeMessage(w, "Profile updated.")
}

// adminCapacityHandler serves POST /admin/capacity?hotelId=&rooms=. A
// capacity below the rooms reserved on some night is rejected (409).
func (s *Server) adminCapacityHandler(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	hotelId := r.FormValue("hotelId")
	rooms, err := strconv.ParseInt(r.FormValue("rooms"), 10, 32)
	if hotelId == "" || err != nil || rooms < 0 {
		http.Error(w, "Please specify hotelId and a non-negative rooms param", http.StatusBadRequest)
		return
	}
	_, err = s.reservationClient.SetCapacity(r.Context(), &reservation.CapacityRequest{
		HotelId: hotelId, Rooms: int32(rooms),
	})
	if err != nil {
		writeGrpcError(w, err)
		return
	}
	writeMessage(w, "Capacity updated.")
}

// NewHotel is the JSON body of POST /admin/hotel.
type NewHotel struct {
	HotelId     string `json:"hotelId"`
	Name        string `json:"name"`
	PhoneNumber string `json:"phoneNumber"`
	Description string `json:"description"`
	Address     struct {
		StreetNumber string `json:"streetNumber"`
		StreetName   string `json:"streetName"`
		City         string `json:"city"`
		State        string `json:"state"`
		Country      string `json:"country"`
		PostalCode   string `json:"postalCode"`
	} `json:"address"`
	Lat       float64       `json:"lat"`
	Lon       float64       `json:"lon"`
	Rating    float64       `json:"rating"`
	Price     float64       `json:"price"`
	Rooms     int32         `json:"rooms"`
	RatePlans []NewRatePlan `json:"ratePlans"`
}

// NewRatePlan is one rate plan of a NewHotel.
type NewRatePlan struct {
	Code     string `json:"code"`
	InDate   string `json:"inDate"`
	OutDate  string `json:"outDate"`
	RoomType struct {
		Code               string  `json:"code"`
		RoomDescription    string  `json:"roomDescription"`
		BookableRate       float64 `json:"bookableRate"`
		TotalRate          float64 `json:"totalRate"`
		TotalRateInclusive float64 `json:"totalRateInclusive"`
	} `json:"roomType"`
}

// Validate checks a new hotel's fields.
func (h *NewHotel) Validate() error {
	if !validHotelId(h.HotelId) {
		return fmt.Errorf("hotelId must be 1-64 letters, digits or '-'")
	}
	if h.Name == "" || h.PhoneNumber == "" {
		return fmt.Errorf("name and phoneNumber are required")
	}
	if !finite(h.Lat) || !finite(h.Lon) || math.Abs(h.Lat) > 90 || math.Abs(h.Lon) > 180 {
		return fmt.Errorf("lat and lon must be a location")
	}
	if !finite(h.Rating) || !finite(h.Price) || h.Rating < 0 || h.Price < 0 || h.Rooms < 0 {
		return fmt.Errorf("rating, price and rooms must be non-negative")
	}
	if len(h.RatePlans) == 0 {
		return fmt.Errorf("a hotel needs a rate plan")
	}
	codes := make(map[string]struct{}, len(h.RatePlans))
	for _, plan := range h.RatePlans {
		if plan.Code == "" || !checkDataFormat(plan.InDate) || !checkDataFormat(plan.OutDate) {
			return fmt.Errorf("a rate plan needs a code and YYYY-MM-DD dates")
		}
		if _, duplicate := codes[plan.Code]; duplicate {
			return fmt.Errorf("rate plan code %s appears twice", plan.Code)
		}
		codes[plan.Code] = struct{}{}
		room := plan.RoomType
		for _, amount := range []float64{room.BookableRate, room.TotalRate, room.TotalRateInclusive} {
			if !finite(amount) || amount < 0 {
				return fmt.Errorf("rates must be non-negative")
			}
		}
	}
	return nil
}

// adminHotelHandler serves POST /admin/hotel. The owners store the hotel in
// this order: profile, rate plans, capacity, then recommendation and geo.
// Search and recommendations only find a hotel through the last two, so a
// hotel they return already has its profile, plans and capacity. An ID that
// is taken is rejected (409). A failed addition can leave an invisible
// partial hotel whose ID stays taken.
func (s *Server) adminHotelHandler(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	var hotel NewHotel
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&hotel); err != nil {
		http.Error(w, "Please send the hotel as a JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := hotel.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	address := hotel.Address
	if _, err := s.profileClient.AddProfile(ctx, &profile.Hotel{
		Id: hotel.HotelId, Name: hotel.Name, PhoneNumber: hotel.PhoneNumber, Description: hotel.Description,
		Address: &profile.Address{
			StreetNumber: address.StreetNumber, StreetName: address.StreetName, City: address.City,
			State: address.State, Country: address.Country, PostalCode: address.PostalCode,
			Lat: float32(hotel.Lat), Lon: float32(hotel.Lon),
		},
	}); err != nil {
		writeGrpcError(w, err)
		return
	}
	plans := make([]*rate.RatePlan, 0, len(hotel.RatePlans))
	for _, plan := range hotel.RatePlans {
		plans = append(plans, &rate.RatePlan{
			HotelId: hotel.HotelId, Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
			RoomType: &rate.RoomType{
				Code: plan.RoomType.Code, RoomDescription: plan.RoomType.RoomDescription,
				BookableRate: plan.RoomType.BookableRate, TotalRate: plan.RoomType.TotalRate,
				TotalRateInclusive: plan.RoomType.TotalRateInclusive,
			},
		})
	}
	if _, err := s.rateClient.AddRates(ctx, &rate.AddRatesRequest{RatePlans: plans}); err != nil {
		writeGrpcError(w, err)
		return
	}
	if _, err := s.reservationClient.AddCapacity(ctx, &reservation.CapacityRequest{
		HotelId: hotel.HotelId, Rooms: hotel.Rooms,
	}); err != nil {
		writeGrpcError(w, err)
		return
	}
	if _, err := s.recommendationClient.AddHotel(ctx, &recommendation.Hotel{
		HotelId: hotel.HotelId, Lat: hotel.Lat, Lon: hotel.Lon, Rate: hotel.Rating, Price: hotel.Price,
	}); err != nil {
		writeGrpcError(w, err)
		return
	}
	if _, err := s.geoClient.AddHotel(ctx, &geo.Point{
		HotelId: hotel.HotelId, Lat: hotel.Lat, Lon: hotel.Lon,
	}); err != nil {
		writeGrpcError(w, err)
		return
	}
	writeMessage(w, "Hotel added.")
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "Please use POST", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// writeGrpcError answers with the HTTP status of a downstream gRPC error.
func writeGrpcError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.AlreadyExists, codes.FailedPrecondition:
		code = http.StatusConflict
	case codes.Unavailable:
		code = http.StatusServiceUnavailable
	}
	http.Error(w, status.Convert(err).Message(), code)
}

func writeMessage(w http.ResponseWriter, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{"message": message})
}

func parseAmount(text string) (float64, bool) {
	value, err := strconv.ParseFloat(text, 64)
	return value, err == nil && finite(value) && value >= 0
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// validHotelId accepts IDs that are safe as memcached keys and inside the
// reservation service's "<id>_<date>" keys.
func validHotelId(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, char := range id {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
			return false
		}
	}
	return true
}
