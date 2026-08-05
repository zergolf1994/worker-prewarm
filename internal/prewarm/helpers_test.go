package prewarm

import "testing"

func TestNormalizeReferer(t *testing.T) {
	tests := map[string]string{
		"":                    "",
		"fembed.co":           "https://fembed.co/",
		"fembed.co/":          "https://fembed.co/",
		"https://fembed.co":   "https://fembed.co/",
		"https://fembed.co/":  "https://fembed.co/",
		" http://localhost/ ": "http://localhost/",
	}

	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := normalizeReferer(input); got != want {
				t.Fatalf("normalizeReferer(%q) = %q, want %q", input, got, want)
			}
		})
	}
}
