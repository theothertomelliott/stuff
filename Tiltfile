local_resource("Data", cmd='rm -rf ./data && GO111MODULE=on go run ./cmd/historygen -c 3 -d 24h')
local_resource('Prometheus', serve_cmd="docker run -v $(pwd)/data:/prometheus -p 9090:9090 prom/prometheus", resource_deps=["Data"])