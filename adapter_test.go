package s3_adapter_test

import (
	"log"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/stretchr/testify/assert"
	s3_adapter "github.com/tetra12/casbin-s3-adapter"
)

func Test_s3(t *testing.T) {
	conf := s3_adapter.S3Config{
		Endpoint: "http://localhost:9000",
		Region:   "my-region",
		Bucket:   "testbuck",
		Key:      "policy.csv",
	}

	sub := "alice" // the user that wants to access a resource.
	obj := "data1" // the resource that is going to be accessed.
	act := "read"  // the operation that the user performs on the resource.

	model, err := model.NewModelFromString(`
		[request_definition]
		r = sub, obj, act
		[policy_definition]
		p = sub, obj, act
		[policy_effect]
		e = some(where (p.eft == allow))
		[matchers]
		m = r.sub == p.sub && r.obj == p.obj && r.act == p.act
	`)
	if err != nil {
		log.Fatal(err)
	}

	adapter, err := s3_adapter.NewAdapter(conf)
	if err != nil {
		t.Fatal(err)
	}

	e, err := casbin.NewSyncedEnforcer(model, adapter)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Adapter - load policy", func(t *testing.T) {
		err := adapter.LoadPolicy(model)
		if err != nil {
			t.Error(err)
		}
		if model.GetValuesForFieldInPolicy("p", "p", 0)[0] != "alice" {
			t.Fatal("Policy wasn't loaded properly")
		}
	})

	t.Run("Enforcer - read success", func(t *testing.T) {
		ok, err := e.Enforce(sub, obj, act)
		if err != nil {
			t.Error(err)
		}
		assert.True(t, ok)
	})
	t.Run("Enforcer - write fail", func(t *testing.T) {
		act = "write"
		ok, err := e.Enforce(sub, obj, act)
		if err != nil {
			t.Error(err)
		}
		assert.False(t, ok)
	})
	t.Run("SavePolicy - success", func(t *testing.T) {
		conf = s3_adapter.S3Config{
			Endpoint: "http://localhost:9000",
			Region:   "my-region",
			Bucket:   "testbuck",
			Key:      "policy0.csv",
		}

		adapter, err := s3_adapter.NewAdapter(conf)
		if err != nil {
			t.Error(err)
		}

		err = adapter.LoadPolicy(model)
		if err != nil {
			t.Error(err)
		}

		err = adapter.SavePolicy(model)
		if err != nil {
			t.Error(err)
		}
	})
}
