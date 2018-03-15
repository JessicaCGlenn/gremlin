package gremlin

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/satori/go.uuid"
	"encoding/json"
)



func TestQuery(t *testing.T) {
	testquery := "g.V()"
	req := Query(testquery)
	assert.Equal(t, testquery, req.Args.Gremlin)
	assert.NotEmpty(t, req.RequestId)
	u, err := uuid.FromString(req.RequestId)
	assert.Empty(t, err)
	assert.NotEmpty(t, u)
}




func TestReadWrite(t *testing.T) {
	assert.NotEmpty(t, testendpoint)

	cl, err := NewClient(testendpoint)
	assert.Empty(t, err)

	// add the entity
	res, err := cl.ExecQuery("graph.addVertex(label, 'person', 'name', 'matilda')")

	// check the entity exists
	res, err = cl.ExecQuery("g.V().has('name', 'matilda').valueMap()")
	var m []map[string][]interface{}
	err = json.Unmarshal(res, &m)
	assert.Empty(t, err)
	assert.Equal(t, "matilda", m[0]["name"][0])

	// delete the entity
	_, err = cl.ExecQuery("g.V().has('name', 'matilda').drop()")
	assert.Empty(t, err)

	// check that the entity is no longer there
	res, err = cl.ExecQuery("g.V().has('name', 'matilda').valueMap()")
	assert.Empty(t, err)
	assert.Empty(t, res)
}

