package replay

import (
	"io/ioutil"
	"net/http"
	"testing"
)

func TestNewRequest(t *testing.T) {
	t.Run("ValidRequest", func(t *testing.T) {
		req, err := newRequest("url", "source")
		if err != nil {
			t.Fatalf("error got:%q want:nil", err)
		}
		if req.URL.String() != "url" {
			t.Fatalf("got:%s want:url", req.URL.String())
		}
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("error got:%q want:nil", err)
		}
		if string(b) != "source" {
			t.Fatalf("got:%s want:source", b)
		}
	})

	t.Run("InvalidRequest", func(t *testing.T) {
		_, err := newRequest("%zzzzz", "source")
		if err == nil {
			t.Fatalf("error got:nil want:error")
		}
	})
}

func TestHeadersFlag_Set(t *testing.T) {
	h := headersFlag{http.Header{}}
	t.Run("ValidHeader", func(t *testing.T) {
		if err := h.Set("MyHeader:Foo"); err != nil {
			t.Fatalf("error got:%q want:nil", err)
		}
	})
	t.Run("NoValue", func(t *testing.T) {
		if err := h.Set("MyHeader"); err == nil {
			t.Fatalf("error got:nil want:error")
		}
	})
	t.Run("KeyValueAsWhitespaces", func(t *testing.T) {
		if err := h.Set("  :  "); err == nil {
			t.Fatalf("error got:nil want:error")
		}
	})
}
