package dataset

// The vocabulary the oracle's own writes draw added hotels and new names
// from. The seeded data (cities, hotels, users) comes from the dataset file.

// cityTemplate is the fixed description of one city. Coordinates are those of
// the city centre; prices are a typical nightly rate of a three-star room in
// the task's single (unnamed) currency, and tax is the share added to it for
// the tax-inclusive rate.
type cityTemplate struct {
	name, region, country string
	lat, lon              float64
	phone, postal         string // templates: 'x' becomes a digit, 'A' an upper-case letter
	basePrice, tax        float64
	districts, streets    []string
}

var (
	namePrefixes = []string{
		"Grand", "Royal", "Park", "City", "Central", "Plaza", "Metropole", "Palace", "Garden",
		"Harbour", "Riverside", "Crown", "Imperial", "Regent", "Majestic", "Continental", "Astor",
		"Belmont", "Carlton", "Savoy", "Windsor", "Ambassador", "Excelsior", "Atlantic", "Vista",
	}
	nameSuffixes = []string{
		"Hotel", "Hotel", "Hotel", "Inn", "Suites", "Residences", "Boutique Hotel", "Lodge",
		"House", "Hostel", "Resort", "Apartments",
	}
	starAdjectives = [][]string{
		{"budget", "no-frills", "simple"},
		{"casual", "friendly", "affordable"},
		{"modern", "comfortable", "stylish"},
		{"upscale", "elegant", "contemporary"},
		{"luxury", "landmark", "five-star"},
	}
	kinds     = []string{"hotel", "hotel", "boutique hotel", "inn", "guesthouse", "hotel"}
	amenities = []string{
		"a rooftop bar", "free Wi-Fi", "a 24-hour fitness centre", "an indoor pool", "a spa",
		"a buffet breakfast", "a business centre", "family rooms", "airport shuttles",
		"a restaurant serving local dishes", "bike rental", "a garden terrace", "meeting rooms",
		"soundproofed rooms", "a cocktail lounge", "valet parking",
	}
	transit = []string{"a metro station", "the main railway station", "a bus terminal", "a tram stop", "the ferry pier"}

	firstNames = []string{
		"james", "mary", "john", "patricia", "robert", "jennifer", "michael", "linda", "william",
		"elizabeth", "david", "barbara", "richard", "susan", "joseph", "jessica", "thomas", "sarah",
		"charles", "karen", "wei", "fang", "hiroshi", "yuki", "minjun", "seoyeon", "arjun", "priya",
		"mohammed", "fatima", "lucas", "sofia", "mateo", "valentina", "noah", "emma", "liam", "olivia",
		"ahmet", "elif", "giulia", "marco", "chloe", "louis", "anna", "felix", "thabo", "naledi",
	}
	lastNames = []string{
		"smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis", "rodriguez",
		"martinez", "hernandez", "lopez", "gonzalez", "wilson", "anderson", "thomas", "taylor",
		"moore", "jackson", "martin", "lee", "wang", "li", "zhang", "chen", "tanaka", "suzuki",
		"kim", "park", "sharma", "patel", "khan", "silva", "santos", "rossi", "bianchi", "dubois",
		"moreau", "muller", "schmidt", "yilmaz", "kaya", "dlamini", "nkosi", "novak", "jansen",
	}
	passwordAlphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
)

// ratePlanCodes are the plan codes a hotel draws from: RACK (the full public
// rate, always present and valid all year) and discounted or negotiated plans
// valid over part of the year.
var ratePlanCodes = []struct {
	code   string
	factor float64
}{
	{"RACK", 1.00}, {"BAR", 0.95}, {"ADV14", 0.85}, {"AAA", 0.90}, {"GOV", 0.80},
	{"CORP", 0.88}, {"PROMO", 0.75}, {"SENIOR", 0.90},
}

var roomTypes = []struct {
	code, description string
	factor            float64
}{
	{"KNG", "King sized bed", 1.10}, {"QN", "Queen sized bed", 1.00}, {"DBL", "Two double beds", 1.05},
	{"TWN", "Two twin beds", 0.95}, {"STD", "Standard room", 0.90}, {"STE", "Suite", 1.90},
}
