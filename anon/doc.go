// Package anon contains functions and data structures related to the map of anonymized fields. In the elasticsearch
// index and requests context, this map is used to guarantee that we do not expose sensitive information but keep the
// same experimental behavior. For instance, an anonymized request using the anonmap of an index anonymization must must
// hit the same set of shards as the original version.
package anon
