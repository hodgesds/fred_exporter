package main

import (
	"net/http"

	"golang.org/x/time/rate"
)

type RateLimitedRoundTripper struct {
	rt http.RoundTripper
	l  *rate.Limiter
}

func NewRateLimitedRoundTripper(rt http.RoundTripper, r rate.Limit, b int) *RateLimitedRoundTripper {
	return &RateLimitedRoundTripper{
		rt: rt,
		l:  rate.NewLimiter(r, b),
	}
}

func (r *RateLimitedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.l.Wait(req.Context())
	return r.rt.RoundTrip(req)
}
