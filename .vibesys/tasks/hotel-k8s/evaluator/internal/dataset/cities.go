package dataset

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

// cityTable lists the cities in popularity order: hotels and request locations
// are distributed over them by Zipf rank, the first being the most popular.
var cityTable = []cityTemplate{
	{"London", "England", "United Kingdom", 51.5074, -0.1278, "+44 20 xxxx xxxx", "WAx xAA", 210, 0.20,
		[]string{"Covent Garden", "Soho", "Mayfair", "South Bank", "Kensington"},
		[]string{"Strand", "Oxford St", "Piccadilly", "Regent St", "Baker St", "Shaftesbury Ave"}},
	{"Paris", "Ile-de-France", "France", 48.8566, 2.3522, "+33 1 xx xx xx xx", "750xx", 200, 0.10,
		[]string{"Le Marais", "Saint-Germain", "Montmartre", "Opera", "Latin Quarter"},
		[]string{"Rue de Rivoli", "Boulevard Haussmann", "Rue Saint-Honore", "Avenue de l'Opera", "Rue de la Paix"}},
	{"New York", "NY", "United States", 40.7580, -73.9855, "(212) xxx-xxxx", "100xx", 260, 0.1475,
		[]string{"Midtown", "Times Square", "Chelsea", "the Financial District", "SoHo"},
		[]string{"Broadway", "7th Ave", "Lexington Ave", "W 44th St", "Park Ave", "Madison Ave"}},
	{"Tokyo", "Tokyo", "Japan", 35.6812, 139.7671, "+81 3-xxxx-xxxx", "1xx-xxxx", 170, 0.10,
		[]string{"Ginza", "Shinjuku", "Shibuya", "Asakusa", "Marunouchi"},
		[]string{"Chuo-dori", "Harumi-dori", "Meiji-dori", "Yasukuni-dori", "Sotobori-dori"}},
	{"Bangkok", "Bangkok", "Thailand", 13.7563, 100.5018, "+66 2 xxx xxxx", "10xxx", 70, 0.17,
		[]string{"Sukhumvit", "Silom", "Siam", "the Riverside", "Old Town"},
		[]string{"Sukhumvit Rd", "Silom Rd", "Rama I Rd", "Charoen Krung Rd", "Ratchadamri Rd"}},
	{"Dubai", "Dubai", "United Arab Emirates", 25.2048, 55.2708, "+971 4 xxx xxxx", "00000", 160, 0.20,
		[]string{"Downtown", "Dubai Marina", "Deira", "Jumeirah", "Business Bay"},
		[]string{"Sheikh Zayed Rd", "Al Wasl Rd", "Jumeirah Beach Rd", "Al Maktoum Rd", "Financial Centre Rd"}},
	{"Singapore", "Singapore", "Singapore", 1.2903, 103.8520, "+65 6xxx xxxx", "0xxxxx", 190, 0.19,
		[]string{"Marina Bay", "Orchard", "Chinatown", "Clarke Quay", "Bugis"},
		[]string{"Orchard Rd", "Raffles Ave", "Beach Rd", "Bras Basah Rd", "Havelock Rd"}},
	{"Istanbul", "Istanbul", "Turkey", 41.0082, 28.9784, "+90 212 xxx xx xx", "34xxx", 90, 0.10,
		[]string{"Sultanahmet", "Beyoglu", "Taksim", "Besiktas", "Karakoy"},
		[]string{"Istiklal Cd", "Divan Yolu Cd", "Cumhuriyet Cd", "Ordu Cd", "Kennedy Cd"}},
	{"Rome", "Lazio", "Italy", 41.9028, 12.4964, "+39 06 xxxx xxxx", "001xx", 160, 0.10,
		[]string{"Trastevere", "Monti", "the Spanish Steps", "Termini", "Prati"},
		[]string{"Via del Corso", "Via Nazionale", "Via Veneto", "Via Cavour", "Via Condotti"}},
	{"Barcelona", "Catalonia", "Spain", 41.3874, 2.1686, "+34 93 xxx xx xx", "080xx", 150, 0.10,
		[]string{"the Gothic Quarter", "Eixample", "El Born", "Barceloneta", "Gracia"},
		[]string{"La Rambla", "Passeig de Gracia", "Carrer de Balmes", "Via Laietana", "Avinguda Diagonal"}},
	{"Hong Kong", "Hong Kong", "China", 22.2800, 114.1588, "+852 xxxx xxxx", "000000", 170, 0.0,
		[]string{"Central", "Tsim Sha Tsui", "Causeway Bay", "Wan Chai", "Mong Kok"},
		[]string{"Queen's Rd Central", "Nathan Rd", "Hennessy Rd", "Des Voeux Rd", "Canton Rd"}},
	{"Los Angeles", "CA", "United States", 34.0522, -118.2437, "(213) xxx-xxxx", "900xx", 190, 0.155,
		[]string{"Downtown", "Hollywood", "Santa Monica", "Beverly Hills", "Koreatown"},
		[]string{"Wilshire Blvd", "Sunset Blvd", "S Figueroa St", "Hollywood Blvd", "W 7th St"}},
	{"Amsterdam", "North Holland", "Netherlands", 52.3676, 4.9041, "+31 20 xxx xxxx", "10xx AA", 190, 0.125,
		[]string{"the Canal Ring", "De Pijp", "the Jordaan", "Museumplein", "Oost"},
		[]string{"Damrak", "Prinsengracht", "Herengracht", "Rokin", "Leidsestraat"}},
	{"Sydney", "NSW", "Australia", -33.8688, 151.2093, "+61 2 xxxx xxxx", "20xx", 180, 0.10,
		[]string{"Circular Quay", "Darling Harbour", "The Rocks", "Surry Hills", "Bondi"},
		[]string{"George St", "Pitt St", "Elizabeth St", "Macquarie St", "Kent St"}},
	{"Seoul", "Seoul", "South Korea", 37.5665, 126.9780, "+82 2-xxxx-xxxx", "0xxxx", 120, 0.10,
		[]string{"Myeongdong", "Gangnam", "Itaewon", "Hongdae", "Jongno"},
		[]string{"Sejong-daero", "Teheran-ro", "Eulji-ro", "Toegye-ro", "Itaewon-ro"}},
	{"San Francisco", "CA", "United States", 37.7749, -122.4194, "(415) xxx-xxxx", "941xx", 230, 0.16,
		[]string{"Union Square", "SoMa", "Fisherman's Wharf", "Nob Hill", "the Embarcadero"},
		[]string{"Geary St", "Market St", "Mission St", "Powell St", "3rd St", "Post St"}},
	{"Berlin", "Berlin", "Germany", 52.5200, 13.4050, "+49 30 xxxxxxxx", "10xxx", 120, 0.12,
		[]string{"Mitte", "Kreuzberg", "Prenzlauer Berg", "Charlottenburg", "Friedrichshain"},
		[]string{"Unter den Linden", "Friedrichstrasse", "Kurfuerstendamm", "Alexanderplatz", "Torstrasse"}},
	{"Mexico City", "CDMX", "Mexico", 19.4326, -99.1332, "+52 55 xxxx xxxx", "06xxx", 80, 0.19,
		[]string{"the Centro Historico", "Roma", "Condesa", "Polanco", "Zona Rosa"},
		[]string{"Paseo de la Reforma", "Av Juarez", "Calle Madero", "Av Insurgentes", "Av Presidente Masaryk"}},
	{"Cape Town", "Western Cape", "South Africa", -33.9249, 18.4241, "+27 21 xxx xxxx", "80xx", 90, 0.15,
		[]string{"the V&A Waterfront", "the City Bowl", "Green Point", "Sea Point", "Camps Bay"},
		[]string{"Long St", "Bree St", "Strand St", "Beach Rd", "Victoria Rd"}},
	{"Rio de Janeiro", "RJ", "Brazil", -22.9068, -43.1729, "+55 21 xxxx-xxxx", "2xxxx-xxx", 90, 0.05,
		[]string{"Copacabana", "Ipanema", "Leblon", "Botafogo", "Centro"},
		[]string{"Av Atlantica", "Rua Visconde de Piraja", "Av Rio Branco", "Rua Barata Ribeiro", "Av Vieira Souto"}},
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
