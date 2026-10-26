package openmeteo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/upstream"
)

// Response bodies captured from the live API (2026-10-26).
const (
	currentBody = `{"latitude":52.5,"longitude":13.400002,"generationtime_ms":0.82,"utc_offset_seconds":0,"timezone":"GMT","timezone_abbreviation":"GMT","elevation":38.0,"current_units":{"time":"unixtime","interval":"seconds","european_aqi":"EAQI","us_aqi":"USAQI","pm10":"μg/m³","pm2_5":"μg/m³","carbon_monoxide":"μg/m³","nitrogen_dioxide":"μg/m³","sulphur_dioxide":"μg/m³","ozone":"μg/m³","ammonia":"μg/m³","dust":"μg/m³","uv_index":""},"current":{"time":1793030400,"interval":3600,"european_aqi":35,"us_aqi":27,"pm10":15.6,"pm2_5":8.6,"carbon_monoxide":238.0,"nitrogen_dioxide":21.6,"sulphur_dioxide":2.0,"ozone":47.0,"ammonia":null,"dust":0.0,"uv_index":0.00}}`
	hourlyBody  = `{"latitude":52.5,"longitude":13.400002,"elevation":38.0,"hourly_units":{"time":"unixtime","european_aqi":"EAQI","pm2_5":"μg/m³"},"hourly":{"time":[1793030400,1793034000,1793037600],"european_aqi":[35,43,null],"pm2_5":[8.6,9.6,9.6]}}`
	geoBody     = `{"results":[{"id":2950159,"name":"Berlin","latitude":52.52437,"longitude":13.41053,"elevation":74.0,"feature_code":"PPLC","country_code":"DE","admin1_id":2950157,"timezone":"Europe/Berlin","population":3426354,"postcodes":["10967","13347"],"country_id":2921044,"country":"Germany","admin1":"State of Berlin"}],"generationtime_ms":0.19}`
)

func newClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	m := upstream.NewMetrics(prometheus.NewRegistry())
	mk := func(name string) *upstream.Client {
		c, err := upstream.New(upstream.Config{
			Name: name, BaseURL: srv.URL, AttemptTimeout: time.Second, MaxAttempts: 1,
			RateLimit: rate.Inf, MaxConcurrent: 4, MaxResponseBytes: 1 << 20, Metrics: m,
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	return New(mk("air_quality"), mk("geocoding"))
}

func TestCurrent(t *testing.T) {
	var got url.Values
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/air-quality" {
			t.Errorf("path = %s", r.URL.Path)
		}
		got = r.URL.Query()
		_, _ = w.Write([]byte(currentBody))
	})

	cur, err := c.Current(context.Background(), airquality.Coordinates{Latitude: 52.52, Longitude: 13.41})
	if err != nil {
		t.Fatal(err)
	}
	if got.Get("latitude") != "52.52" || got.Get("longitude") != "13.41" || got.Get("timeformat") != "unixtime" || got.Get("current") != variables {
		t.Errorf("query = %v", got)
	}
	r := cur.Current
	if !r.Time.Equal(time.Unix(1793030400, 0)) || r.Time.Location() != time.UTC {
		t.Errorf("time = %v", r.Time)
	}
	if r.EuropeanAQI == nil || *r.EuropeanAQI != (airquality.Index{Value: 35, Category: "fair"}) {
		t.Errorf("eaqi = %+v", r.EuropeanAQI)
	}
	if r.USAQI == nil || r.USAQI.Category != "good" {
		t.Errorf("usaqi = %+v", r.USAQI)
	}
	if r.Pollutants.PM25 == nil || *r.Pollutants.PM25 != 8.6 || r.Pollutants.Ammonia != nil {
		t.Errorf("pollutants = %+v", r.Pollutants)
	}
	if cur.Coordinates.Latitude != 52.5 || cur.Elevation != 38 {
		t.Errorf("grid cell = %+v elevation=%v", cur.Coordinates, cur.Elevation)
	}
	if cur.Units["pm10"] != "μg/m³" || cur.Units["european_aqi"] != "EAQI" {
		t.Errorf("units = %v", cur.Units)
	}
	for _, dropped := range []string{"time", "interval", "uv_index"} {
		if _, ok := cur.Units[dropped]; ok {
			t.Errorf("unit %q should be dropped", dropped)
		}
	}
}

func TestForecast(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("forecast_hours") != "3" || r.URL.Query().Get("hourly") != variables {
			t.Errorf("query = %v", r.URL.Query())
		}
		_, _ = w.Write([]byte(hourlyBody))
	})
	f, err := c.Forecast(context.Background(), airquality.Coordinates{Latitude: 52.52, Longitude: 13.41}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Hourly) != 3 {
		t.Fatalf("len = %d", len(f.Hourly))
	}
	if f.Hourly[1].EuropeanAQI.Value != 43 || f.Hourly[2].EuropeanAQI != nil {
		t.Errorf("nulls must map to nil: %+v %+v", f.Hourly[1].EuropeanAQI, f.Hourly[2].EuropeanAQI)
	}
	if f.Hourly[0].USAQI != nil || f.Hourly[0].Pollutants.PM10 != nil {
		t.Error("missing columns must map to nil")
	}
}

func TestContractViolations(t *testing.T) {
	tests := map[string]struct {
		body string
		call func(*Client) error
	}{
		"current missing": {`{"latitude":1}`, func(c *Client) error {
			_, err := c.Current(context.Background(), airquality.Coordinates{})
			return err
		}},
		"hourly missing": {`{"latitude":1}`, func(c *Client) error {
			_, err := c.Forecast(context.Background(), airquality.Coordinates{}, 1)
			return err
		}},
		"ragged hourly": {`{"hourly":{"time":[1,2],"pm10":[1]}}`, func(c *Client) error {
			_, err := c.Forecast(context.Background(), airquality.Coordinates{}, 2)
			return err
		}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tt.body)) })
			err := tt.call(c)
			var uerr *upstream.Error
			if !errors.As(err, &uerr) || uerr.Kind != upstream.KindBadResponse || !uerr.Unavailable() {
				t.Fatalf("err = %v, want bad_response", err)
			}
		})
	}
}

func TestGeocode(t *testing.T) {
	var got url.Values
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/search" {
			t.Errorf("path = %s", r.URL.Path)
		}
		got = r.URL.Query()
		_, _ = w.Write([]byte(geoBody))
	})
	locs, err := c.Geocode(context.Background(), "berlin", "DE")
	if err != nil {
		t.Fatal(err)
	}
	if got.Get("name") != "berlin" || got.Get("countryCode") != "DE" || got.Get("count") != "1" {
		t.Errorf("query = %v", got)
	}
	want := airquality.Location{
		Name: "Berlin", Admin1: "State of Berlin", Country: "Germany", CountryCode: "DE",
		Timezone: "Europe/Berlin", Population: 3426354,
		Coordinates: airquality.Coordinates{Latitude: 52.52437, Longitude: 13.41053},
	}
	if len(locs) != 1 || locs[0] != want {
		t.Errorf("locs = %+v", locs)
	}
}

func TestGeocodeNoResults(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("countryCode") {
			t.Error("countryCode must be omitted when empty")
		}
		_, _ = w.Write([]byte(`{"generationtime_ms":0.2}`))
	})
	locs, err := c.Geocode(context.Background(), "zzzzqqqxx", "")
	if err != nil || len(locs) != 0 || locs == nil {
		t.Fatalf("locs=%v err=%v; want empty non-nil slice", locs, err)
	}
}

func TestUpstreamErrorsPropagate(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":true,"reason":"Latitude must be in range of -90 to 90°. Given: 152.52."}`))
	})
	_, err := c.Current(context.Background(), airquality.Coordinates{Latitude: 152.52})
	var uerr *upstream.Error
	if !errors.As(err, &uerr) || uerr.Kind != upstream.KindClient || uerr.Reason == "" {
		t.Fatalf("err = %v", err)
	}
}
