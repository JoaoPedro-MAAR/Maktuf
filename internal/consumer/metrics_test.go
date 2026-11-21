package consumer

import "testing"

func TestSensorGroup(t *testing.T) {
	for id, want := range map[string]string{
		"sensor-007":      "sensor",
		"agressivo-001":   "agressivo",
		"grupo-lento-012": "grupo-lento",
		"semhifen":        "semhifen",
		"-001":            "-001",
		"sensor-":         "sensor",
	} {
		if got := sensorGroup(id); got != want {
			t.Errorf("sensorGroup(%q) = %q, esperado %q", id, got, want)
		}
	}
}
