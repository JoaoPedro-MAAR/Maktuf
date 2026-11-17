package broker

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
	}{
		{"sensores/a/temperatura", "sensores/a/temperatura", true},
		{"sensores/a/temperatura", "sensores/b/temperatura", false},
		{"sensores/+/temperatura", "sensores/a/temperatura", true},
		{"sensores/+/temperatura", "sensores/b/temperatura", true},
		{"sensores/+/temperatura", "sensores/a/b/temperatura", false},
		{"sensores/+/temperatura", "sensores/temperatura", false},
		{"sensores/+/temperatura", "sensores/a/umidade", false},
		{"sensores/+/temperatura", "sensores//temperatura", false},
		{"+/+", "a/b", true},
		{"+/+", "a", false},
		{"+/+", "a/b/c", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/d", false},
		{"a/+/c", "x/b/c", false},
		{"+", "sensores", true},
		{"sensores", "sensores/a", false},
	}
	for _, tc := range cases {
		if got := match(tc.filter, tc.topic); got != tc.want {
			t.Errorf("match(%q, %q) = %v, esperado %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}
