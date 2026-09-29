package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

type Config struct {
	ProxyAddr    string
	AdminAddr    string
	DrainTimeout time.Duration
	Initial      []UpstreamConfig
}

type App struct {
	config      Config
	pool        *Pool
	shadows     *ShadowManager
	proxyServer *http.Server
	adminServer *http.Server
}

func NewApp(config Config) *App {
	pool := NewPool()
	shadows := NewShadowManager()
	if _, err := pool.Publish(config.Initial); err != nil {
		log.Fatalf("invalid initial upstreams: %v", err)
	}

	proxyRouter := chi.NewRouter()
	proxyRouter.Handle("/*", NewGatewayHandler(pool, shadows))

	return &App{
		config:  config,
		pool:    pool,
		shadows: shadows,
		proxyServer: &http.Server{
			Addr:    config.ProxyAddr,
			Handler: proxyRouter,
		},
		adminServer: &http.Server{
			Addr:    config.AdminAddr,
			Handler: NewAdminRouter(pool, shadows),
		},
	}
}

func (a *App) Run() error {
	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	proxyErr := make(chan error, 1)
	adminErr := make(chan error, 1)
	go func() { proxyErr <- a.serve(a.proxyServer, "proxy") }()
	go func() { adminErr <- a.serve(a.adminServer, "admin") }()

	select {
	case err := <-proxyErr:
		return err
	case err := <-adminErr:
		return err
	case <-shutdownCtx.Done():
		return a.Shutdown()
	}
}

func (a *App) serve(server *http.Server, name string) error {
	log.Printf("%s listening on %s", name, server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (a *App) Shutdown() error {
	log.Println("shutting down and draining proxy requests")
	a.pool.Shutdown()
	drainCtx, cancel := context.WithTimeout(context.Background(), a.config.DrainTimeout)
	defer cancel()

	proxyDone := make(chan error, 1)
	shadowDone := make(chan struct{})
	go func() {
		proxyDone <- a.proxyServer.Shutdown(drainCtx)
	}()
	go func() {
		a.shadows.Shutdown(drainCtx)
		close(shadowDone)
	}()

	select {
	case err := <-proxyDone:
		<-shadowDone
		return a.shutdownAdmin(err)
	case <-drainCtx.Done():
		log.Println("drain timeout; canceling remaining requests")
		err := a.proxyServer.Close()
		_ = a.adminServer.Close()
		<-shadowDone
		<-proxyDone
		return err
	}
}

func (a *App) shutdownAdmin(proxyErr error) error {
	adminCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.adminServer.Shutdown(adminCtx); err != nil {
		_ = a.adminServer.Close()
		if proxyErr == nil {
			return err
		}
	}
	return proxyErr
}
