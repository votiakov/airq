// Package airquality is the domain layer: vendor-neutral models, AQI classification,
// and the caching service that sits between the HTTP API and the data provider.
package airquality

import (
	"math"
	"strconv"
	"time"
)

// Coordinates is a WGS84 position.
type Coordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// coordinatePrecision is the number of decimals coordinates are rounded to before
// caching and querying. 2 decimals is ~1.1 km, far finer than the CAMS models behind
// Open-Meteo (~11 km in Europe, ~45 km globally), so rounding loses no information
// while letting nearby requests share cache entries.
const coordinatePrecision = 2

// Normalize rounds coordinates to coordinatePrecision decimals.
func (c Coordinates) Normalize() Coordinates {
	p := math.Pow10(coordinatePrecision)
	round := func(v float64) float64 {
		r := math.Round(v*p) / p
		if r == 0 {
			return 0 // fold -0 into 0 so both share a cache key
		}
		return r
	}
	return Coordinates{Latitude: round(c.Latitude), Longitude: round(c.Longitude)}
}

// Key is a stable cache key for normalized coordinates.
func (c Coordinates) Key() string {
	return strconv.FormatFloat(c.Latitude, 'f', coordinatePrecision, 64) + "," +
		strconv.FormatFloat(c.Longitude, 'f', coordinatePrecision, 64)
}

// Location is a geocoded place.
type Location struct {
	Name        string `json:"name"`
	Admin1      string `json:"admin1,omitempty"`
	Country     string `json:"country,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	Timezone    string `json:"timezone,omitempty"`
	Population  int    `json:"population,omitempty"`
	Coordinates
}

// Index is an air quality index value with its category.
type Index struct {
	Value    int    `json:"value"`
	Category string `json:"category"`
}

// Pollutants holds concentrations in μg/m³. A nil value means the model provides no
// data for that pollutant at this location (e.g. ammonia outside Europe).
type Pollutants struct {
	PM10            *float64 `json:"pm10"`
	PM25            *float64 `json:"pm2_5"`
	CarbonMonoxide  *float64 `json:"carbon_monoxide"`
	NitrogenDioxide *float64 `json:"nitrogen_dioxide"`
	SulphurDioxide  *float64 `json:"sulphur_dioxide"`
	Ozone           *float64 `json:"ozone"`
	Ammonia         *float64 `json:"ammonia"`
	Dust            *float64 `json:"dust"`
}

// Reading is the air quality at one point in time.
type Reading struct {
	Time        time.Time  `json:"time"`
	EuropeanAQI *Index     `json:"european_aqi"`
	USAQI       *Index     `json:"us_aqi"`
	UVIndex     *float64   `json:"uv_index"`
	Pollutants  Pollutants `json:"pollutants"`
}

// Current is the latest reading for a model grid cell.
type Current struct {
	// Coordinates of the model grid cell that answered, which differ slightly from the
	// requested position.
	Coordinates Coordinates       `json:"coordinates"`
	Elevation   float64           `json:"elevation"`
	Units       map[string]string `json:"units"`
	Current     Reading           `json:"current"`
}

// Forecast is an hourly forecast for a model grid cell.
type Forecast struct {
	Coordinates Coordinates       `json:"coordinates"`
	Elevation   float64           `json:"elevation"`
	Units       map[string]string `json:"units"`
	Hourly      []Reading         `json:"hourly"`
}

// MaxForecastHours is the longest forecast served. The service always fetches this
// horizon and slices it per request so every forecast query for a location shares
// one cache entry.
const MaxForecastHours = 96

// NewEuropeanAQI classifies a European AQI value
// (https://open-meteo.com/en/docs/air-quality-api).
func NewEuropeanAQI(v *float64) *Index {
	if v == nil {
		return nil
	}
	n := int(math.Round(*v))
	var cat string
	switch {
	case n <= 20:
		cat = "good"
	case n <= 40:
		cat = "fair"
	case n <= 60:
		cat = "moderate"
	case n <= 80:
		cat = "poor"
	case n <= 100:
		cat = "very_poor"
	default:
		cat = "extremely_poor"
	}
	return &Index{Value: n, Category: cat}
}

// NewUSAQI classifies a US EPA AQI value.
func NewUSAQI(v *float64) *Index {
	if v == nil {
		return nil
	}
	n := int(math.Round(*v))
	var cat string
	switch {
	case n <= 50:
		cat = "good"
	case n <= 100:
		cat = "moderate"
	case n <= 150:
		cat = "unhealthy_for_sensitive_groups"
	case n <= 200:
		cat = "unhealthy"
	case n <= 300:
		cat = "very_unhealthy"
	default:
		cat = "hazardous"
	}
	return &Index{Value: n, Category: cat}
}
