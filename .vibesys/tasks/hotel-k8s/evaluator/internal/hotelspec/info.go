package hotelspec

// The infos below are the contract between the accuracy workload and this oracle: the JSON
// object each operation's request carries as its Info (kept in kept.binpb), by operation name.
// The workload writes them; the oracle reads them strictly (an unknown field is an error).
//
//	search                 SearchInfo     GET /hotels
//	reserve                ReserveInfo    GET /reservation
//	reserve_capacity_hotel ReserveInfo    GET /reservation on a hotel whose capacity the workload writes
//	set_capacity           CapacityInfo   POST /admin/capacity
//	set_profile            ProfileInfo    POST /admin/profile
//	set_rate               RateInfo       POST /admin/rate
//	rates                  HotelInfo      GET /rates
//	recommend_dis          RecommendInfo  GET /recommendations?require=dis
//	recommend_rate         RecommendInfo  GET /recommendations?require=rate
//	recommend_price        RecommendInfo  GET /recommendations?require=price
//	login                  LoginInfo      GET /user
//
// Every operation is one HTTP step.
type (
	// SearchInfo: the stay's nights (YYYY-MM-DD) and geo's nearest hotels with rate plans at the
	// searched point, in order (what search may return, from the seeded data).
	SearchInfo struct {
		Nights     []string `json:"nights"`
		Candidates []string `json:"candidates"`
	}
	// ReserveInfo: the hotel, the stay's nights (one night: see the package comment), the rooms
	// and whether the user's password was right.
	ReserveInfo struct {
		Hotel  string   `json:"hotel"`
		Nights []string `json:"nights"`
		Rooms  int      `json:"rooms"`
		Valid  bool     `json:"valid"`
	}
	CapacityInfo struct {
		Hotel string `json:"hotel"`
		Rooms int    `json:"rooms"`
	}
	// ProfileInfo: the name ("" when unchanged) and the phone written.
	ProfileInfo struct {
		Hotel string `json:"hotel"`
		Name  string `json:"name"`
		Phone string `json:"phone"`
	}
	// RateInfo: the plan's code and its new bookable, total and inclusive rates.
	RateInfo struct {
		Hotel  string     `json:"hotel"`
		Code   string     `json:"code"`
		Prices [3]float64 `json:"prices"`
	}
	HotelInfo struct {
		Hotel string `json:"hotel"`
	}
	// RecommendInfo: the hotels the answer must hold (from the seeded data) for the requirement.
	RecommendInfo struct {
		Require  string   `json:"require"`
		Expected []string `json:"expected"`
	}
	LoginInfo struct {
		Valid bool `json:"valid"`
	}
)
