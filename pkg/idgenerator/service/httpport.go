package service

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/wwq1988/idgenerator/pkg/service"
)

// HTTPPort HTTPPort
func HTTPPort(service service.Service) http.Handler {
	r := mux.NewRouter().StrictSlash(true).SkipClean(true)
	sub := r.PathPrefix("/v1").Subrouter().StrictSlash(true).SkipClean(true)
	sub.HandleFunc("/newid", newIDHTTPPort(service.NewID)).Methods(http.MethodPost)
	return r
}

func newIDHTTPPort(newID service.NewIDFunc) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &service.NewIDRequest{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			log.Printf("failed to decode request,err:%#v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp, err := newID(r.Context(), req)
		if err != nil {
			log.Printf("failed to newID,err:%#v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("failed to encode resp,err:%#v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

}
