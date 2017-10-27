package anon

import (
	"regexp"
	"strings"
)

// FieldsRegexp keeps the regular expressions to be applied while anonymizing fields.
type FieldsRegexp map[string]*regexp.Regexp

const regexpSeparator = "::"

// MustParseFieldsRE is like FieldsRegexpFromStringSlice but panics in case of errors.
func MustParseFieldsRE(fSlice []string) FieldsRegexp {
	f, err := FieldsRegexpFromStringSlice(fSlice)
	if err != nil {
		panic(err)
	}
	return f
}

// FieldsRegexpFromStringSlice creates a new, populated FieldRegexp from a string slice.
// The field name-regexp separator is "::".
func FieldsRegexpFromStringSlice(fSlice []string) (FieldsRegexp, error) {
	ret := make(FieldsRegexp)
	for _, f := range fSlice {
		parts := strings.Split(f, regexpSeparator)
		if len(parts) > 1 {
			re, err := regexp.Compile(parts[1])
			if err != nil {
				return nil, err
			}
			ret[parts[0]] = re
		} else {
			ret[parts[0]] = regexp.MustCompile(".*")
		}
	}
	return ret, nil
}
