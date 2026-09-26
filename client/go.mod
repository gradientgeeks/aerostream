module github.com/gradientgeeks/aerostream/client

go 1.22

replace github.com/gradientgeeks/aerostream/go-controller => ../go-controller

require (
	github.com/gradientgeeks/aerostream/go-controller v0.0.0
	google.golang.org/grpc v1.64.0
)

require (
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
)
