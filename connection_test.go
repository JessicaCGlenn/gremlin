package gremlin

import (
	"testing"
	"os"
	"github.com/stretchr/testify/assert"
	"encoding/json"
	"fmt"
)

var testendpoint = getEndpoint()
var testfailingendpoint = getFailingEndpoint()

func getEndpoint() string {
	if str, ok := os.LookupEnv("GREMLIN_SERVER"); ok {
		return str
	}
	return "ws://localhost:8182/gremlin"
}

// todo : perhaps there is a better way to mock this
func getFailingEndpoint() string {
	if str, ok := os.LookupEnv("FAILING_GREMLIN_SERVER"); ok {
		return str
	}
	return "ws://localhostt:8812/gremlin"
}

// as a user I want to be able to set a single host connection
func TestSingleConnection(t *testing.T) {

	// create a connection without errors
	cl, err := NewClient(testendpoint)
	assert.Empty(t, err)
	assert.NotEmpty(t, cl)

	// execute a simple math query to prove connection is stable
	res, err := cl.ExecQuery("1 + 1")
	assert.Empty(t, err)
	var m []map[string]interface{}
	err = json.Unmarshal(res, &m)
	assert.Empty(t, err)
	assert.Equal(t, 2.0, m[0]["@value"])
}

// as a user I want my client to be threadsafe
func TestThreadsafeClient(t *testing.T) {
	assert.NotEmpty(t, testendpoint)
	cl, err := NewClient(testendpoint)
	assert.Empty(t, err)
	done := make(chan struct{})
	concurrency := 100
	for i := 0; i < concurrency; i++ {
		go func(c *Client, d chan struct{}, n int) {
			res, err := c.ExecQuery(fmt.Sprintf("%v * %v", n, n))
			assert.Empty(t, err)
			var m []map[string]interface{}
			err = json.Unmarshal(res, &m)
			assert.Empty(t, err)
			assert.Equal(t, float64(n * n), m[0]["@value"])
			if n == (concurrency - 1) {
				close(d)
			}
		}(cl, done, i)
	}
	<- done
}

func BenchmarkSimpleRequest(b *testing.B) {
	c, err := NewClient(testendpoint)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < b.N; i++ {
		n := i
		res, _ := c.ExecQuery(fmt.Sprintf("%v * %v", n, n))
		var m []map[string]interface{}
		json.Unmarshal(res, &m)
	}
}
