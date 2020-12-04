package main

import (
	"datawaves/handlers"
	"datawaves/util"
	"net/http"
	"os"

	"github.com/rs/cors"
	muxtrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gorilla/mux"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	handlers.ParseTemplates()

	if util.IsProduction() {
		tracer.Start(tracer.WithAgentAddr(os.Getenv("DD_TRACE_AGENT_HOSTNAME")), tracer.WithAnalytics(true), tracer.WithEnv("prod"))
	}

	router := muxtrace.NewRouter(muxtrace.WithServiceName("datawaves"))

	handlers.Handle(
		router,
		handlers.General,
		handlers.Users,
		handlers.Projects,
		handlers.APIV1,
		handlers.Crons,
		handlers.Tasks,
	)

	r := cors.Default().Handler(router)
	http.Handle("/", r)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		panic(err)
	}
}
