package s3_adapter

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casbin/casbin/v2/model"
)

var ErrNYI = errors.New("NYI - not yet implemented")

type S3Config struct {
	Endpoint string
	Region   string
	Bucket   string
}

type Adapter struct {
	client *s3.Client
	bucket string
}

// NewAdapter - factory for S3 adapter
func NewAdapter(cfg S3Config) (*Adapter, error) {
	// The EndpointResolver is used for compatibility with MinIO
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               cfg.Endpoint,
			HostnameImmutable: true, // Needs to be true for MinIO
		}, nil
	})

	config, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(resolver))
	if err != nil {
		return nil, err
	}

	out := &Adapter{
		client: s3.NewFromConfig(config),
		bucket: cfg.Bucket,
	}

	return out, nil
}

func (sa *Adapter) LoadPolicy(model model.Model) error {
	return ErrNYI
}

func (sa *Adapter) SavePolicy(model model.Model) error {
	return ErrNYI
}

func (sa *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	return ErrNYI
}

func (sa *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return ErrNYI
}

func (sa *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return ErrNYI
}
