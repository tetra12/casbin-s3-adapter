package s3_adapter

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"github.com/casbin/casbin/v2/util"
)

var ErrNYI = errors.New("NYI - not yet implemented")

type S3Config struct {
	Endpoint string
	Region   string
	Bucket   string
	Key      string
}

type Adapter struct {
	client *s3.Client
	cfg    S3Config
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
		cfg:    cfg,
	}

	return out, nil
}

// LoadPolicy - loads policy from S3 bucket
func (a *Adapter) LoadPolicy(model model.Model) error {

	res, err := a.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &a.cfg.Bucket,
		Key:    &a.cfg.Key,
	})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	buf := bufio.NewReader(res.Body)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		persist.LoadPolicyLine(line, model)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// SavePolicy - saves policy to a S3 bucket
func (a *Adapter) SavePolicy(model model.Model) error {
	var tmp bytes.Buffer
	var streamLength int64

	// process policies
	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			l, err := tmp.WriteString(fmt.Sprintf("%s, %s\n", ptype, util.ArrayToString(rule)))
			if err != nil {
				return err
			}
			streamLength += int64(l)
		}
	}

	// process groups
	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			l, err := tmp.WriteString(fmt.Sprintf("%s, %s\n", ptype, util.ArrayToString(rule)))
			if err != nil {
				return err
			}
			streamLength += int64(l)
		}
	}

	_, err := a.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &a.cfg.Bucket,
		Key:    &a.cfg.Key,
		Body:   &tmp,
	}, s3.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware))
	if err != nil {
		return err
	}

	return nil
}

func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	return ErrNYI
}

func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return ErrNYI
}

func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return ErrNYI
}
