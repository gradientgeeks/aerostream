.PHONY: all build build-controller build-broker build-client clean start stop status certs

all: build

build: build-controller build-broker build-client

build-controller:
	@echo "Building Go Controller..."
	@cd go-controller && go build -o bin/controller cmd/controller/main.go

build-broker:
	@echo "Building Rust Broker..."
	@cd rust-broker && cargo build --release

build-client:
	@echo "Building Go Client..."
	@cd client && go build -o bin/client main.go

clean:
	@echo "Cleaning binaries and data directories..."
	@rm -rf go-controller/bin/
	@rm -rf client/bin/
	@rm -rf data/
	@rm -f *.log

start: build
	@echo "Starting AeroMQ Cluster (3 Go Controllers, 2 Rust Brokers)..."
	@mkdir -p data/broker_1 data/broker_2
	
	# Start Controllers
	@setsid ./go-controller/bin/controller -id node1 -raft-addr 127.0.0.1:7001 -grpc-addr 127.0.0.1:8001 -http-addr 127.0.0.1:9001 -ui-dir ./ui/dist/ui/browser -bootstrap > controller1.log 2>&1 < /dev/null &
	@sleep 1
	@setsid ./go-controller/bin/controller -id node2 -raft-addr 127.0.0.1:7002 -grpc-addr 127.0.0.1:8002 -http-addr 127.0.0.1:9002 -join http://127.0.0.1:9001 > controller2.log 2>&1 < /dev/null &
	@setsid ./go-controller/bin/controller -id node3 -raft-addr 127.0.0.1:7003 -grpc-addr 127.0.0.1:8003 -http-addr 127.0.0.1:9003 -join http://127.0.0.1:9001 > controller3.log 2>&1 < /dev/null &
	@sleep 2
	
	# Start Brokers
	@setsid ./rust-broker/target/release/rust-broker --id 1 --host 127.0.0.1 --data-port 9091 --kafka-port 9093 --controller http://127.0.0.1:8001 --storage-dir ./data/broker_1 > broker1.log 2>&1 < /dev/null &
	@setsid ./rust-broker/target/release/rust-broker --id 2 --host 127.0.0.1 --data-port 9092 --kafka-port 9094 --controller http://127.0.0.1:8001 --storage-dir ./data/broker_2 > broker2.log 2>&1 < /dev/null &
	@sleep 2
	
	@echo "AeroMQ Cluster is running!"


stop:
	@echo "Stopping AeroMQ cluster..."
	@-pkill -f go-controller/bin/controller || true
	@-pkill -f target/release/rust-broker || true

status:
	@echo "--- Go Controllers Status ---"
	@curl -s http://127.0.0.1:9001/status || echo "Node 1 Offline"
	@curl -s http://127.0.0.1:9002/status || echo "Node 2 Offline"
	@curl -s http://127.0.0.1:9003/status || echo "Node 3 Offline"

# Generate a self-signed CA and a server cert (SAN 127.0.0.1 / localhost) shared
# by the controller and brokers. For local TLS testing only.
certs:
	@mkdir -p config/certs
	@openssl req -x509 -newkey rsa:2048 -nodes -keyout config/certs/ca.key \
		-out config/certs/ca.crt -days 365 -subj "/CN=AeroMQ-CA" 2>/dev/null
	@openssl req -newkey rsa:2048 -nodes -keyout config/certs/server.key \
		-out config/certs/server.csr -subj "/CN=aeromq" 2>/dev/null
	@printf "subjectAltName=IP:127.0.0.1,DNS:localhost" > config/certs/san.ext
	@openssl x509 -req -in config/certs/server.csr -CA config/certs/ca.crt \
		-CAkey config/certs/ca.key -CAcreateserial -out config/certs/server.crt \
		-days 365 -extfile config/certs/san.ext 2>/dev/null
	@echo "Generated config/certs/{ca.crt,server.crt,server.key}"
