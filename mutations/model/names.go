package model

import (
	"regexp"
	"strings"
)

var segmentRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// validName reports whether s is a dotted name of non-empty segments of letters, digits, _ and -.
func validName(s string) bool {
	for _, seg := range strings.Split(s, ".") {
		if !segmentRe.MatchString(seg) {
			return false
		}
	}
	return true
}

func isPattern(s string) bool { return strings.Contains(s, "*") }

// validPattern reports whether s is a dotted name whose segments are names, `*` or `**`.
func validPattern(s string) bool {
	for _, seg := range strings.Split(s, ".") {
		if seg != "*" && seg != "**" && !segmentRe.MatchString(seg) {
			return false
		}
	}
	return true
}

// matchPattern reports whether name matches pattern: `*` is one segment, `**` any number.
func matchPattern(pattern, name string) bool {
	return matchSegments(strings.Split(pattern, "."), strings.Split(name, "."))
}

func matchSegments(p, n []string) bool {
	if len(p) == 0 {
		return len(n) == 0
	}
	if p[0] == "**" {
		for k := 0; k <= len(n); k++ {
			if matchSegments(p[1:], n[k:]) {
				return true
			}
		}
		return false
	}
	if len(n) == 0 {
		return false
	}
	return (p[0] == "*" || p[0] == n[0]) && matchSegments(p[1:], n[1:])
}
