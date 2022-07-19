package main

import (
	"context"
	"strings"

	zinc "github.com/zinclabs/sdk-go-zincsearch"
)

type Zinc struct {
	client *zinc.APIClient
	user   string
	pass   string
}

func NewZinc(host string, user, pass string) (*Zinc, error) {
	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: "http://" + strings.TrimPrefix(host, "http://"),
		},
	}

	client := zinc.NewAPIClient(configuration)
	return &Zinc{
		client: client,
		user:   user,
		pass:   pass,
	}, nil
}

func (z *Zinc) Version() (string, error) {
	resp, _, err := z.client.Default.Version(context.Background()).Execute()
	if err != nil {
		return "", err
	}
	return resp.GetVersion(), nil
}

func (z *Zinc) IndexDocument(index string, document map[string]interface{}) (string, error) {
	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: z.user,
		Password: z.pass,
	})
	resp, _, err := z.client.Document.Index(ctx, index).Document(document).Execute()
	if err != nil {
		return "", err
	}
	return resp.GetId(), nil
}
