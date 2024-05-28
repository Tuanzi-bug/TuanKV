package client

import (
	"github.com/hdt3213/godis/lib/logger"
	"testing"
)

func TestClient(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:8080")
	if err != nil {
		t.Error(err)
	}
	client.Start()
}
