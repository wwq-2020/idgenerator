package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wwq1988/idgenerator/pkg/idgetter"
	"github.com/wwq1988/idgenerator/pkg/leadership"
	"github.com/wwq1988/idgenerator/pkg/service"
)

type client struct {
	leadership leadership.Leadership
	httpClient *http.Client
}

// Factory Factory
type Factory func(leadership leadership.Leadership) idgetter.IDGetter

// CreateFactory CreateFactory
func CreateFactory(httpClient *http.Client) Factory {
	return func(leadership leadership.Leadership) idgetter.IDGetter {
		return New(httpClient, leadership)
	}
}

// New New
func New(httpClient *http.Client, leadership leadership.Leadership) idgetter.IDGetter {
	return &client{
		leadership: leadership,
		httpClient: httpClient,
	}
}

func (c *client) GetID(ctx context.Context, biz string) (int64, error) {
	addr, err := c.leadership.GetLeaderAddr(ctx, biz)
	if err != nil {
		return 0, err
	}
	url := fmt.Sprintf("http://%s/v1/newid", addr)
	req := &service.NewIDRequest{}
	data, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respObj := &service.NewIDResp{}
	if err := json.NewDecoder(resp.Body).Decode(respObj); err != nil {
		return 0, err
	}
	return respObj.ID, nil
}
