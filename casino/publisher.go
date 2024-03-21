package casino

import (
	"context"
)

type Publisher struct {
	exg    string
	client *Client
}

func (p *Publisher) Publish(routeKey string, msg *Publishing) error {
	ctx := context.Background()
	return p.client.Publish(ctx, p.exg, routeKey, msg)
}

func (p *Publisher) PublishWithContext(ctx context.Context, routeKey string, msg *Publishing) error {
	return p.client.Publish(ctx, p.exg, routeKey, msg)
}

func (p *Publisher) PublishData(routeKey string, data []byte) error {
	ctx := context.Background()
	return p.client.Publish(ctx, p.exg, routeKey, &Publishing{
		ContentType: "text/plain",
		Body:        data,
	})
}

func (p *Publisher) PublishDataWithContext(ctx context.Context, routeKey string, data []byte) error {
	return p.client.Publish(ctx, p.exg, routeKey, &Publishing{
		ContentType: "text/plain",
		Body:        data,
	})
}
