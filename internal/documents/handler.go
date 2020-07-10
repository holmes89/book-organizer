package documents

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/holmes89/book-organizer/internal/common"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

func MakeDocumentHandler(mr *mux.Router, service DocumentService) http.Handler {
	r := mr.PathPrefix("/documents").Subrouter()

	h := &documentHandler{
		service: service,
	}

	r.HandleFunc("/{id}", h.FindByID).Methods("GET")
	r.HandleFunc("/{id}", h.UpdateFields).Methods("PATCH")
	r.HandleFunc("/{id}", h.Delete).Methods("DELETE")
	r.HandleFunc("/scan", h.Scan).Methods("PUT")
	r.HandleFunc("/", h.FindAll).Methods("GET")

	return r
}

type documentHandler struct {
	service DocumentService
}

func (h *documentHandler) FindByID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)

	id, ok := vars["id"]

	if !ok {
		common.MakeError(w, http.StatusBadRequest, "document", "Missing Id", "findbyid")
		return
	}

	entity, err := h.service.FindByID(ctx, id)

	if err != nil {
		common.MakeError(w, http.StatusInternalServerError, "document", "Server Error", "findbyid")
		return
	}

	common.EncodeResponse(r.Context(), w, entity)
}

func (h *documentHandler) UpdateFields(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	b, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	req := Document{}
	if err := json.Unmarshal(b, &req); err != nil {
		logrus.WithError(err).Error("unable to unmarshal link tag")
		common.MakeError(w, http.StatusBadRequest, "document", "Bad Request", "updateFields")
		return
	}

	vars := mux.Vars(r)

	id, ok := vars["id"]

	if !ok {
		common.MakeError(w, http.StatusBadRequest, "document", "Missing Id", "updateFields")
		return
	}

	entity, err := h.service.UpdateFields(ctx, id, req)

	if err != nil {
		common.MakeError(w, http.StatusInternalServerError, "document", "Server Error", "updateFields")
		return
	}

	common.EncodeResponse(r.Context(), w, entity)
}

func (h *documentHandler) Delete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)

	id, ok := vars["id"]

	if !ok {
		common.MakeError(w, http.StatusBadRequest, "document", "Missing Id", "delete")
		return
	}

	if err := h.service.Delete(ctx, id); err != nil {
		common.MakeError(w, http.StatusInternalServerError, "document", "Server Error", "delete")
		return
	}

	common.EncodeResponse(r.Context(), w, map[string]string{"status": "success"})
}

func (h *documentHandler) FindAll(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	entity, err := h.service.FindAll(ctx, nil)

	if err != nil {
		common.MakeError(w, http.StatusInternalServerError, "document", "Server Error", "findall")
		return
	}

	common.EncodeResponse(r.Context(), w, entity)
}

func (h *documentHandler) Scan(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	err := h.service.Scan(ctx)

	if err != nil {
		common.MakeError(w, http.StatusInternalServerError, "document", "Server Error", "scan")
		return
	}

	common.EncodeResponse(r.Context(), w, map[string]string{"status": "success"})
}
