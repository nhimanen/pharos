// Package model holds the domain types.
package model

import "strings"

// Base carries shared identity fields.
type Base struct {
	ID string
}

// User is a person known to the system.
type User struct {
	Base
	Name  string `json:"name"`
	Email string `json:"email,omitempty"`
	tags  map[string]string
}

// DisplayName returns a printable form of the user's name.
func (u User) DisplayName() string {
	return strings.TrimSpace(u.Name)
}

// Tag stores a tag and returns the new tag count.
func (u *User) Tag(key, value string) int {
	if u.tags == nil {
		u.tags = map[string]string{}
	}
	u.tags[key] = value
	return len(u.tags)
}

// Sanitize strips brace characters from name.
func Sanitize(name string) string {
	if strings.ContainsRune(name, '{') {
		name = strings.ReplaceAll(name, "{", "")
	}
	return strings.Trim(name, "}")
}
