// Package openmeteo adapts the Open-Meteo Air Quality and Geocoding APIs
// (https://open-meteo.com/en/docs/air-quality-api) to the airquality domain. It is
// the anti-corruption layer: vendor wire formats never leak past this package.
package openmeteo

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/upstream"
)

// Attribution must accompany the data (CC BY 4.0, https://open-meteo.com/en/license).
const Attribution = "Air quality data by Open-Meteo.com (CC BY 4.0), based on Copernicus Atmosphere Monitoring Service (CAMS)"

const (
	airQualityPath = "/v1/air-quality"
	geocodingPath  = "/v1/search"
	// variables requested for both current and hourly data.
	variables = "european_aqi,us_aqi,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,ammonia,dust,uv_index"
)

// Getter is the subset of upstream.Client used here.
type Getter interface {
	GetJSON(ctx context.Context, path string, query url.Values, out any) error
}

// Client implements airquality.Provider.
type Client struct {
	airQuality Getter
	geocoding  Getter
}

var _ airquality.Provider = (*Client)(nil)

// New returns a Client using separate upstream clients (and therefore separate circuit
// breakers, quotas and bulkheads) for the two Open-Meteo hosts, so a geocoding outage
// cannot stop coordinate-based lookups.
func New(airQuality, geocoding Getter) *Client {
	return &Client{airQuality: airQuality, geocoding: geocoding}
}

// Current implements airquality.Provider.
func (c *Client) Current(ctx context.Context, coords airquality.Coordinates) (airquality.Current, error) {
	q := baseQuery(coords)
	q.Set("current", variables)

	var resp aqResponse
	if err := c.airQuality.GetJSON(ctx, airQualityPath, q, &resp); err != nil {
		return airquality.Current{}, err
	}
	if resp.Current == nil {
		return airquality.Current{}, contractError("air_quality", "response has no current block")
	}
	return airquality.Current{
		Coordinates: airquality.Coordinates{Latitude: resp.Latitude, Longitude: resp.Longitude},
		Elevation:   resp.Elevation,
		Units:       units(resp.CurrentUnits),
		Current:     resp.Current.reading(),
	}, nil
}

// Forecast implements airquality.Provider.
func (c *Client) Forecast(ctx context.Context, coords airquality.Coordinates, hours int) (airquality.Forecast, error) {
	q := baseQuery(coords)
	q.Set("hourly", variables)
	q.Set("forecast_hours", strconv.Itoa(hours))

	var resp aqResponse
	if err := c.airQuality.GetJSON(ctx, airQualityPath, q, &resp); err != nil {
		return airquality.Forecast{}, err
	}
	if resp.Hourly == nil {
		return airquality.Forecast{}, contractError("air_quality", "response has no hourly block")
	}
	readings, err := resp.Hourly.readings()
	if err != nil {
		return airquality.Forecast{}, contractError("air_quality", err.Error())
	}
	return airquality.Forecast{
		Coordinates: airquality.Coordinates{Latitude: resp.Latitude, Longitude: resp.Longitude},
		Elevation:   resp.Elevation,
		Units:       units(resp.HourlyUnits),
		Hourly:      readings,
	}, nil
}

// Geocode implements airquality.Provider. It returns an empty slice when nothing
// matches (Open-Meteo omits "results" entirely in that case).
func (c *Client) Geocode(ctx context.Context, name, countryCode string) ([]airquality.Location, error) {
	q := url.Values{
		"name":     {name},
		"count":    {"1"},
		"language": {"en"},
		"format":   {"json"},
	}
	if countryCode != "" {
		q.Set("countryCode", countryCode)
	}
	var resp geoResponse
	if err := c.geocoding.GetJSON(ctx, geocodingPath, q, &resp); err != nil {
		return nil, err
	}
	locs := make([]airquality.Location, 0, len(resp.Results))
	for _, r := range resp.Results {
		locs = append(locs, airquality.Location{
			Name:        r.Name,
			Admin1:      r.Admin1,
			Country:     r.Country,
			CountryCode: r.CountryCode,
			Timezone:    r.Timezone,
			Population:  r.Population,
			Coordinates: airquality.Coordinates{Latitude: r.Latitude, Longitude: r.Longitude},
		})
	}
	return locs, nil
}

func baseQuery(c airquality.Coordinates) url.Values {
	return url.Values{
		"latitude":   {strconv.FormatFloat(c.Latitude, 'f', -1, 64)},
		"longitude":  {strconv.FormatFloat(c.Longitude, 'f', -1, 64)},
		"timezone":   {"GMT"},
		"timeformat": {"unixtime"}, // unambiguous instants instead of zone-less local times
	}
}

// contractError reports a 200 response that does not match the documented schema.
// It is classified as a bad response so it is eligible for stale fallback.
func contractError(upstreamName, msg string) error {
	return &upstream.Error{Upstream: upstreamName, Kind: upstream.KindBadResponse, Err: fmt.Errorf("contract violation: %s", msg)}
}

// units keeps only pollutant/index units (dropping time/interval bookkeeping).
func units(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if k == "time" || k == "interval" || v == "" {
			continue
		}
		out[k] = v
	}
	return out
}

type aqResponse struct {
	Latitude     float64           `json:"latitude"`
	Longitude    float64           `json:"longitude"`
	Elevation    float64           `json:"elevation"`
	CurrentUnits map[string]string `json:"current_units"`
	Current      *aqPoint          `json:"current"`
	HourlyUnits  map[string]string `json:"hourly_units"`
	Hourly       *aqSeries         `json:"hourly"`
}

type aqPoint struct {
	Time            int64    `json:"time"`
	EuropeanAQI     *float64 `json:"european_aqi"`
	USAQI           *float64 `json:"us_aqi"`
	PM10            *float64 `json:"pm10"`
	PM25            *float64 `json:"pm2_5"`
	CarbonMonoxide  *float64 `json:"carbon_monoxide"`
	NitrogenDioxide *float64 `json:"nitrogen_dioxide"`
	SulphurDioxide  *float64 `json:"sulphur_dioxide"`
	Ozone           *float64 `json:"ozone"`
	Ammonia         *float64 `json:"ammonia"`
	Dust            *float64 `json:"dust"`
	UVIndex         *float64 `json:"uv_index"`
}

func (p aqPoint) reading() airquality.Reading {
	return airquality.Reading{
		Time:        time.Unix(p.Time, 0).UTC(),
		EuropeanAQI: airquality.NewEuropeanAQI(p.EuropeanAQI),
		USAQI:       airquality.NewUSAQI(p.USAQI),
		UVIndex:     p.UVIndex,
		Pollutants: airquality.Pollutants{
			PM10:            p.PM10,
			PM25:            p.PM25,
			CarbonMonoxide:  p.CarbonMonoxide,
			NitrogenDioxide: p.NitrogenDioxide,
			SulphurDioxide:  p.SulphurDioxide,
			Ozone:           p.Ozone,
			Ammonia:         p.Ammonia,
			Dust:            p.Dust,
		},
	}
}

// aqSeries is Open-Meteo's column-oriented hourly data; values may be null.
type aqSeries struct {
	Time            []int64    `json:"time"`
	EuropeanAQI     []*float64 `json:"european_aqi"`
	USAQI           []*float64 `json:"us_aqi"`
	PM10            []*float64 `json:"pm10"`
	PM25            []*float64 `json:"pm2_5"`
	CarbonMonoxide  []*float64 `json:"carbon_monoxide"`
	NitrogenDioxide []*float64 `json:"nitrogen_dioxide"`
	SulphurDioxide  []*float64 `json:"sulphur_dioxide"`
	Ozone           []*float64 `json:"ozone"`
	Ammonia         []*float64 `json:"ammonia"`
	Dust            []*float64 `json:"dust"`
	UVIndex         []*float64 `json:"uv_index"`
}

func (s *aqSeries) readings() ([]airquality.Reading, error) {
	n := len(s.Time)
	columns := map[string][]*float64{
		"european_aqi": s.EuropeanAQI, "us_aqi": s.USAQI, "pm10": s.PM10, "pm2_5": s.PM25,
		"carbon_monoxide": s.CarbonMonoxide, "nitrogen_dioxide": s.NitrogenDioxide,
		"sulphur_dioxide": s.SulphurDioxide, "ozone": s.Ozone, "ammonia": s.Ammonia,
		"dust": s.Dust, "uv_index": s.UVIndex,
	}
	for name, col := range columns {
		// A missing column is tolerated (treated as all-null); a ragged one is not.
		if col != nil && len(col) != n {
			return nil, fmt.Errorf("hourly %s has %d values for %d timestamps", name, len(col), n)
		}
	}
	at := func(col []*float64, i int) *float64 {
		if col == nil {
			return nil
		}
		return col[i]
	}
	out := make([]airquality.Reading, n)
	for i := range n {
		out[i] = aqPoint{
			Time:            s.Time[i],
			EuropeanAQI:     at(s.EuropeanAQI, i),
			USAQI:           at(s.USAQI, i),
			PM10:            at(s.PM10, i),
			PM25:            at(s.PM25, i),
			CarbonMonoxide:  at(s.CarbonMonoxide, i),
			NitrogenDioxide: at(s.NitrogenDioxide, i),
			SulphurDioxide:  at(s.SulphurDioxide, i),
			Ozone:           at(s.Ozone, i),
			Ammonia:         at(s.Ammonia, i),
			Dust:            at(s.Dust, i),
			UVIndex:         at(s.UVIndex, i),
		}.reading()
	}
	return out, nil
}

type geoResponse struct {
	Results []struct {
		Name        string  `json:"name"`
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
		CountryCode string  `json:"country_code"`
		Country     string  `json:"country"`
		Admin1      string  `json:"admin1"`
		Timezone    string  `json:"timezone"`
		Population  int     `json:"population"`
	} `json:"results"`
}
