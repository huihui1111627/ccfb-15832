package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"toposcheduler/internal/sim"
	"toposcheduler/internal/web"
)

func main() {
	seed := flag.Int64("seed", 20261125, "fixed random seed for the simulated cluster")
	addr := flag.String("addr", ":18088", "HTTP listen address")
	flag.Parse()

	if port := os.Getenv("PORT"); port != "" && *addr == ":18088" {
		*addr = ":" + port
	}

	simulation, err := sim.NewDemo(*seed)
	if err != nil {
		log.Fatalf("initialize simulation: %v", err)
	}
	server := web.New(simulation)

	log.Printf("无人机容器拓扑调度模拟器已启动: http://localhost%s", *addr)
	log.Printf("固定种子: %d", *seed)
	if err := http.ListenAndServe(*addr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
