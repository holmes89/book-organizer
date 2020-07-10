package common

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"net/http"
	"strings"
)

func MakeError(w http.ResponseWriter, code int, domain string, message string, method string) {
	logrus.WithFields(
		logrus.Fields{
			"type":   code,
			"domain": domain,
			"method": method,
		}).Error(strings.ToLower(message))
	http.Error(w, message, code)
}

func EncodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return enc.Encode(response)
}
