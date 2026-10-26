package model

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// UnsetMarker stands for a setting that has no value; it differs from the hash of an empty value.
const UnsetMarker = "unset"

// SettingDigest is a tracked setting's name with the hash of its value, or UnsetMarker.
type SettingDigest struct {
	Name string
	Hash string
}

// SettingHash hashes a value for the digest of the statement at position (1-based) of a
// mutation. A nil value is unset.
func SettingHash(namespace, mutation string, position int, name string, value *string) string {
	if value == nil {
		return UnsetMarker
	}
	h := sha256.New()
	var n [8]byte
	for _, part := range []string{namespace, mutation, name, *value} {
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		h.Write(n[:])
		h.Write([]byte(part))
	}
	binary.BigEndian.PutUint64(n[:], uint64(position))
	h.Write(n[:])
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// Digest encodes the data hash and the tracked settings of a statement as the text dmut stores.
func Digest(dataHash string, settings []SettingDigest) string {
	var b strings.Builder
	b.WriteString(dataHash)
	for _, s := range settings {
		b.WriteByte('\n')
		b.WriteString(s.Name)
		b.WriteByte('\t')
		b.WriteString(s.Hash)
	}
	return b.String()
}

// ParseDigest is the reverse of Digest.
func ParseDigest(digest string) (dataHash string, settings []SettingDigest) {
	lines := strings.Split(digest, "\n")
	for _, line := range lines[1:] {
		name, hash, _ := strings.Cut(line, "\t")
		settings = append(settings, SettingDigest{Name: name, Hash: hash})
	}
	return lines[0], settings
}
