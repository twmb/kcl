package userscram

import (
	"slices"
	"testing"
)

func TestSplitPassword(t *testing.T) {
	for _, test := range []struct {
		in       string
		pairs    []string
		password string
		ok       bool
	}{
		{"user=a,mechanism=m,password=p", []string{"user=a", "mechanism=m"}, "p", true},
		{"user=a,mechanism=m,password=a,b=c\"", []string{"user=a", "mechanism=m"}, "a,b=c\"", true},
		{"user=a,PASSWORD=x,y", []string{"user=a"}, "x,y", true},
		{"password=,", nil, ",", true},
		{"password=p,user=a", nil, "p,user=a", true},
		{"user=a,mechanism=m", []string{"user=a", "mechanism=m"}, "", false},
		{"user=a,mypassword=p", []string{"user=a", "mypassword=p"}, "", false},
	} {
		t.Run(test.in, func(t *testing.T) {
			pairs, password, ok := splitPassword(test.in)
			if !slices.Equal(pairs, test.pairs) || password != test.password || ok != test.ok {
				t.Errorf("got (%q, %q, %v), want (%q, %q, %v)", pairs, password, ok, test.pairs, test.password, test.ok)
			}
		})
	}
}
