package anon

import "strings"

// TODO(danielfireman): Improve constructor and re-evaluate if those fields need to be exported.
// NOTE: It needs a better API.

// Anonymizer anonymizes arbitrary object fields based on regular expressions.
type Anonymizer struct {
	FMap FieldsMap
	FRE  FieldsRegexp
}

// Anonymize anonymizes the passed-in objects updating its fields.
func (a *Anonymizer) Anonymize(objSlice ...map[string]interface{}) {
	for _, object := range objSlice {
		for fieldName, v := range object {
			switch v.(type) {
			case map[string]interface{}:
				a.Anonymize(v.(map[string]interface{}))
			case []interface{}:
				for _, subItem := range v.([]interface{}) {
					if s, ok := subItem.(map[string]interface{}); ok {
						a.Anonymize(s)
					}
				}
			}
			if re, ok := a.FRE[fieldName]; ok {
				vStr, ok := v.(string)
				if ok {
					match := re.FindStringSubmatch(vStr)
					if len(match) > 1 {
						key := match[1]
						anonV := a.FMap.Anonymize(fieldName, key)
						object[fieldName] = strings.Replace(vStr, key, anonV, -1)
					}
				}
			}
		}
	}
}

// WriteFieldsMapToFile writes a JSON version of the anonymized fields map to the specified file path.
func (a *Anonymizer) WriteFieldsMapToFile(path string) error {
	return a.FMap.WriteJSONToFile(path)
}
