package anon

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
)

// FieldsMap maps fields to their associated anonymized values.
type FieldsMap map[string]map[string]string

// MustReadFieldsMap it is like ReadFieldsMapFromFile, but panic in case of errors.
func MustReadFieldsMap(path string) FieldsMap {
	m, err := ReadFieldsMapFromFile(path)
	if err != nil {
		panic(err)
	}
	return m
}

// ReadFieldsMapFromFile loads the map of anonymized fields from a file path.
func ReadFieldsMapFromFile(path string) (FieldsMap, error) {
	if path == "" {
		return make(FieldsMap), nil
	}
	var fields FieldsMap
	c, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(c, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// Anonymize returns an anonymous version of the field key and updates the
// backing map, making sure that sucessive calls of this method return the same
// anonymous value.
func (fields FieldsMap) Anonymize(fieldName, key string) string {
	innerMap, ok := fields[fieldName]
	if !ok {
		innerMap = make(map[string]string)
		fields[fieldName] = innerMap
	}
	anon, ok := innerMap[key]
	if !ok {
		// Incremental numbers are fast and good enough for this kind of anonymization.
		// No way to reverse, unless they have the map.
		anon = strconv.FormatInt(int64(len(innerMap)), 10)
	}
	innerMap[key] = anon
	return anon
}

// WriteJSONToFile writes a JSON version of the anonymized fields map to the specified file path.
func (fields FieldsMap) WriteJSONToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := json.MarshalIndent(fields, "", "  ")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	w.WriteString(string(buf))
	w.Flush()
	return nil
}
