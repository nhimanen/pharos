// Package service renders greetings.
package service

import (
	"fmt"
	"strings"
)

// Greeter renders greetings for users.
type Greeter struct {
	prefix string
	Sep    string
}

// Formatter turns a name into a greeting.
type Formatter interface {
	Format(name string) string
}

// Namer reports a display name.
type Namer interface {
	Name() string
}

// FormatterNamer is anything that can both format and name itself.
type FormatterNamer interface {
	Formatter
	Namer
}

// Loud shouts its greetings by embedding Greeter.
type Loud struct {
	Greeter
}

// NewGreeter returns a Greeter with sane defaults.
func NewGreeter() *Greeter {
	return &Greeter{prefix: "Hello", Sep: ", "}
}

// Greet greets the given name.
func (g *Greeter) Greet(name string) string {
	return g.join(g.prefix, name)
}

func (g *Greeter) join(parts ...string) string {
	return strings.Join(parts, g.Sep)
}

// Format satisfies Formatter.
func (g *Greeter) Format(name string) string {
	return fmt.Sprintf("%s%s", g.prefix, name)
}

// Describe returns an opaque descriptor value.
func Describe() struct{ Kind string } {
	return struct{ Kind string }{Kind: "greeter"}
}

// Map applies f to each element of in.
func Map[T, U any](in []T, f func(T) U) []U {
	out := make([]U, 0, len(in))
	for _, v := range in {
		out = append(out, f(v))
	}
	return out
}
