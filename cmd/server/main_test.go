package main

import "testing"

func TestToDecimalParsesFormattedAmounts(t *testing.T) {
	tests := map[string]string{
		"1234567.89":             "1234567.89",
		"1 234 567,89":          "1234567.89",
		"1\u00a0234\u00a0567,89": "1234567.89",
		"1.234.567,89":          "1234567.89",
		"1,234,567.89":          "1234567.89",
		"1 234 567":             "1234567",
		"1.234.567":             "1234567",
		"1,234,567":             "1234567",
		"1234567,89 тг":         "1234567.89",
		"-1 234,50":             "-1234.5",
		"5.6888806E+06":         "5688880.6",
		"5,6888806E+06":         "5688880.6",
		"1.0665008e+007":        "10665008",
	}

	for input, want := range tests {
		got := toDecimal(input).String()
		if got != want {
			t.Fatalf("toDecimal(%q) = %q, want %q", input, got, want)
		}
	}
}
