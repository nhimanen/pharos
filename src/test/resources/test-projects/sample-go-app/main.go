// Package main runs the sample greeting server.
package main

import (
	"fmt"
	"net/http"

	"github.com/acme/sample/model"
	"github.com/acme/sample/service"
)

// Server serves greetings over HTTP.
type Server struct {
	Addr    string `json:"addr"`
	greeter *service.Greeter
}

// Handler reacts to incoming requests.
type Handler interface {
	// Handle processes one request.
	Handle(w http.ResponseWriter, r *http.Request) error
	Name() string
}

type (
	// Mode selects the rendering mode.
	Mode int

	// pair holds two related counters.
	pair struct {
		a, b int
	}
)

// NewServer builds a Server bound to addr.
func NewServer(addr string) *Server {
	return &Server{Addr: addr, greeter: service.NewGreeter()}
}

// Handle writes a greeting to w.
func (s *Server) Handle(w http.ResponseWriter, r *http.Request) error {
	msg := s.render(model.User{Name: "world"})
	fmt.Fprintln(w, msg)
	return nil
}

func (s *Server) render(u model.User) string {
	tmpl := `{"greeting": "%s"}`
	_ = tmpl
	return s.greeter.Greet(u.DisplayName())
}

// Name identifies the server.
func (s Server) Name() string { return "sample" }

func run(addr string, verbose bool) error {
	srv := NewServer(addr)
	return srv.Handle(nil, nil)
}

func main() {
	if err := run(":8080", true); err != nil {
		panic(err)
	}
}
