package gremlin

import (
	"sync"
	"time"
	"github.com/satori/go.uuid"
	"net/url"
	"strings"
	"errors"
)


var (
	EndpointOnIceError = errors.New("endpoint on ice, try again later")
	EmptyUrlError = errors.New("missing url for this endpoint")
)


func NewEndpoint(urlStr string) (*sync.Map, error) {

	m := sync.Map{}
	id, err := uuid.NewV4()

	m.Store("Id", id)
	m.Store("Url", urlStr)
	m.Store("LastResponse", 0)
	m.Store("ErrorScore", 0)
	m.Store("OnIceUntil", time.Now())
	return &m, err
}


func newEndpointsChannel(urlStr string) (*sync.Map, chan *sync.Map, error) {
	endpoints, err := SplitServers(urlStr)
	if err != nil {
		return nil, nil, err
	}
	endpointmap := sync.Map{}
	endpointchannel := make(chan *sync.Map, len(endpoints))


	for _, endpoint := range endpoints {
		// make sure that we aren't doubling up on the same endpoint
		if _, exists := endpointmap.Load(endpoint.String()); exists {
			continue
		}

		if val, err := NewEndpoint(endpoint.String()); err == nil {
			endpointmap.Store(endpoint.String(), val)
			endpointchannel <- val
		}
	}
	if len(endpointchannel) == 0 {
		return nil, nil, NoEndpointsError
	}
	return &endpointmap, endpointchannel, nil
}


// if a string is provided as a comma seperated list then we should be able to create a cluster from that
func SplitServers(connString string) (servers []*url.URL, err error) {
	serverStrings := strings.Split(connString, ",")
	if len(serverStrings) < 1 {
		err = MalformedClusterStringErr
		return
	}
	for _, serverString := range serverStrings {
		var u *url.URL
		if u, err = url.Parse(strings.TrimSpace(serverString)); err != nil {
			return
		}
		servers = append(servers, u)
	}
	return
}


func findValidEndpoint(c chan *sync.Map, mu *sync.RWMutex) (*sync.Map, error) {
	return selectEndpoint(c, nil, mu)
}

func selectEndpoint(c chan *sync.Map, firstid *uuid.UUID, mu *sync.RWMutex) (*sync.Map, error) {
	// todo : this might require the sync mutex when retrieving from the channel
	mu.Lock()
	m := <- c
	c <- m
	mu.Unlock()
	thisid, err := idForEndpoint(m)
	if err != nil {
		return nil, err
	}
	if firstid != nil && firstid.String() == thisid.String() {
		return nil, EndpointOnIceError
	}
	if endpointOnIce(m) {
		// find the next endpoint
		eid, err := idForEndpoint(m)
		if err != nil {
			return nil, err
		}
		// put the endpoint back in the queue
		if len(c) == 1 {
			// if there is only one endpoint in the channel then make sure to return instead of
			// trying recursion
			return nil, EndpointOnIceError
		}

		// if there is only one in the queue then this won't work, and if none are available
		// then this will freeze
		if firstid == nil {
			firstid = eid
		}
		sm, err := selectEndpoint(c, firstid, mu)
		if err != nil {
			return nil, err
		}
		if neid, err := idForEndpoint(sm); err != nil || neid == eid {
			if err != nil {
				return nil, err
			} else {
				return nil, EndpointOnIceError
			}
		}
		return sm, nil
	}
	return m, nil
}


// if the endpoint keeps erroring out it will be put 'on ice' and will not be available
// for a length of time. The amount of time it is out for will be determined by 'putEndpointOnIce'
func endpointOnIce(m *sync.Map) bool {
	val, exists := m.Load("OnIceUntil")
	if exists && (val.(time.Time)).After(time.Now()) {
		return true
	}
	return false
}

func putEndpointOnIce(m *sync.Map) {
	// todo : does this require a sync mutex?

	// error score
	var errorScore int
	val, ex := m.Load("ErrorScore")
	if ex {
		errorScore = val.(int)
	} else {
		errorScore = 0
	}

	// new error score
	errorScore = errorScore + 1
	iceInt := (errorScore * errorScore) * 5
	onIceUntil := time.Now().Add(time.Duration(iceInt) * time.Second)
	m.Store("OnIceUntil", onIceUntil)
	m.Store("ErrorScore", errorScore)
}

func reduceErrorScore(m *sync.Map) {
	// error score
	var errorScore int
	val, ex := m.Load("ErrorScore")
	if ex {
		errorScore = val.(int)
	} else {
		errorScore = 0
	}
	// new error score
	if errorScore > 0 {
		errorScore = errorScore - 1
	}
	m.Store("ErrorScore", errorScore)
}

func urlStrForEndpoint(m *sync.Map) (string, error) {
	val, ex := m.Load("Url")
	if !ex {
		return "", EmptyUrlError
	}
	return val.(string), nil
}

func idForEndpoint(m *sync.Map) (*uuid.UUID, error) {
	val, exists := m.Load("Id")
	if exists {
		id := val.(uuid.UUID)
		return &id, nil
	} else {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		m.Store("Id", id)
		return &id, nil
	}
}
