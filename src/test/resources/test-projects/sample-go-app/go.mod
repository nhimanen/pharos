module github.com/acme/sample

go 1.22

require (
	github.com/google/uuid v1.6.0
	golang.org/x/sync v0.7.0 // indirect
)

require github.com/pkg/errors v0.9.1

replace github.com/acme/shared => ./shared
