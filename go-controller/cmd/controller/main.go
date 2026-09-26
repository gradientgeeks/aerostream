package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	appconfig "github.com/gradientgeeks/aerostream/go-controller/pkg/config"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/consensus"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/grpcserver"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
	pb "github.com/gradientgeeks/aerostream/go-controller/proto/aeromq"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "Path to a TOML config file (CLI flags override file values)")
	nodeID := flag.String("id", "", "Unique node ID")
	raftAddr := flag.String("raft-addr", "", "Raft communication address")
	grpcAddr := flag.String("grpc-addr", "", "gRPC API service address")
	httpAddr := flag.String("http-addr", "", "HTTP control API address")
	dataDir := flag.String("data-dir", "", "Directory to store Raft snapshots (in-memory if empty)")
	uiDir := flag.String("ui-dir", "", "Directory containing built Web UI console assets (optional)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new Raft cluster")
	joinAddr := flag.String("join", "", "Join address of an existing controller (e.g. http://127.0.0.1:9001)")
	flag.Parse()

	// Load config file (or defaults), then apply explicit CLI overrides.
	cfg, err := appconfig.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *raftAddr != "" {
		cfg.RaftAddr = *raftAddr
	}
	if *grpcAddr != "" {
		cfg.GRPCAddr = *grpcAddr
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *bootstrap {
		cfg.Bootstrap = true
	}
	if *joinAddr != "" {
		cfg.Join = *joinAddr
	}

	log.Printf("[AeroMQ Controller] Starting node %s (TLS=%v, auth=%v)...",
		cfg.NodeID, cfg.TLS.Enabled, cfg.Auth.Token != "")

	// Initialize Raft Node
	raftNode, err := consensus.NewRaftNode(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}

	// Set up gRPC Server
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("gRPC listen failed: %v", err)
	}
	var serverOpts []grpc.ServerOption
	if creds, err := cfg.TLS.ServerCredentials(); err != nil {
		log.Fatalf("Failed to configure TLS: %v", err)
	} else if creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(creds))
		log.Printf("[AeroMQ Controller] gRPC TLS enabled")
	}
	if cfg.Auth.Token != "" {
		serverOpts = append(serverOpts, grpc.UnaryInterceptor(grpcserver.TokenAuthInterceptor(cfg.Auth.Token)))
		log.Printf("[AeroMQ Controller] gRPC token authentication enabled")
	}
	grpcServer := grpc.NewServer(serverOpts...)
	serverImpl := grpcserver.NewServer(raftNode, cfg.Cluster.FailureDetectionInterval())
	
	pb.RegisterControlServiceServer(grpcServer, serverImpl)
	pb.RegisterDiscoveryServiceServer(grpcServer, serverImpl)

	// Run gRPC Server in background
	go func() {
		log.Printf("[AeroMQ Controller] gRPC API server listening on %s", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Handle Join HTTP API and stats
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		addr := r.URL.Query().Get("addr")
		if id == "" || addr == "" {
			http.Error(w, "missing id or addr parameters", http.StatusBadRequest)
			return
		}
		
		log.Printf("[AeroMQ Controller] Join request received from node %s (%s)", id, addr)
		if err := raftNode.Join(id, addr); err != nil {
			log.Printf("[AeroMQ Controller] Failed to join node %s: %v", id, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("joined successfully"))
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		meta := raftNode.FSM.GetMetadata(nil)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\n  \"node_id\": \"%s\",\n  \"state\": \"%s\",\n  \"leader\": \"%s\",\n",
			cfg.NodeID, raftNode.Raft.State().String(), raftNode.Raft.Leader())
		fmt.Fprintf(w, "  \"brokers_count\": %d,\n  \"topics_count\": %d\n}\n", 
			len(meta.Brokers), len(meta.Topics))
	})

	// Register REST API endpoints for Web UI
	restServer := rest.NewServer(raftNode, cfg.HTTPAddr)
	restServer.RegisterRoutes(http.DefaultServeMux)

	// Serve Web UI Console if ui-dir is provided and valid
	if *uiDir != "" {
		resolvedUIDir, err := filepath.Abs(*uiDir)
		if err == nil {
			if fi, err := os.Stat(resolvedUIDir); err == nil && fi.IsDir() {
				indexPath := filepath.Join(resolvedUIDir, "index.html")
				spaHandler := func(prefix string) http.HandlerFunc {
					return func(w http.ResponseWriter, r *http.Request) {
						trimmed := strings.TrimPrefix(r.URL.Path, prefix)
						if trimmed == "" || trimmed == "/" {
							http.ServeFile(w, r, indexPath)
							return
						}
						targetPath := filepath.Join(resolvedUIDir, filepath.Clean(trimmed))
						if fi, err := os.Stat(targetPath); err == nil && !fi.IsDir() {
							http.ServeFile(w, r, targetPath)
							return
						}
						// SPA route fallback to index.html
						http.ServeFile(w, r, indexPath)
					}
				}

				// Primary AeroStream Console endpoint
				http.HandleFunc("/aerostream/console", func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/aerostream/console/", http.StatusMovedPermanently)
				})
				http.HandleFunc("/aerostream/console/", spaHandler("/aerostream/console/"))

				// Backward compatibility alias: /aeromq/console
				http.HandleFunc("/aeromq/console", func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/aerostream/console/", http.StatusMovedPermanently)
				})
				http.HandleFunc("/aeromq/console/", func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/aerostream/console/", http.StatusMovedPermanently)
				})

				// Root redirect to console
				http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/" {
						http.Redirect(w, r, "/aerostream/console/", http.StatusFound)
						return
					}
					http.NotFound(w, r)
				})
				log.Printf("[AeroStream Controller] Web UI Console active at http://%s/aerostream/console (serving %s)", cfg.HTTPAddr, resolvedUIDir)
			} else {
				log.Printf("[AeroMQ Controller] Warning: ui-dir %s is not an accessible directory", *uiDir)
			}
		}
	}

	// Run HTTP server
	go func() {
		log.Printf("[AeroMQ Controller] HTTP API server listening on %s", cfg.HTTPAddr)
		if err := http.ListenAndServe(cfg.HTTPAddr, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// If join address is provided, try to join the cluster
	if cfg.Join != "" {
		go func() {
			// Small delay to ensure our own Raft transport is ready
			time.Sleep(1 * time.Second)
			joinUrl := fmt.Sprintf("%s/join?id=%s&addr=%s", cfg.Join, cfg.NodeID, cfg.RaftAddr)
			log.Printf("[AeroMQ Controller] Attempting to join cluster via %s", joinUrl)
			
			client := http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(joinUrl)
			if err != nil {
				log.Printf("[AeroMQ Controller] Join attempt failed: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("[AeroMQ Controller] Join attempt returned non-OK status: %s", resp.Status)
				return
			}
			log.Printf("[AeroMQ Controller] Successfully joined the Raft cluster!")
		}()
	}

	// Block forever
	select {}
}
