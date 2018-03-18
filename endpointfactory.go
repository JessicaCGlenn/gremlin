package gremlin

import (
	"sync"
	"github.com/gorilla/websocket"
	"net/http"
	"github.com/satori/go.uuid"
	"github.com/jessicacglenn/pool"
)

type EndpointFactory struct {
	endpoints			chan *sync.Map
	mu 					*sync.Mutex
	endpointmap			*sync.Map
}

func NewEndpointFactory(urlStr string) (ef *EndpointFactory, err error) {
	em, ec, err := newEndpointsChannel(urlStr)
	if err != nil {
		return nil, err
	}

	ef = &EndpointFactory{
		endpoints: ec,
		endpointmap: em,
		mu: &sync.Mutex{},
	}

	return
}

func (f *EndpointFactory) connectSocket() (*websocket.Conn, error) {
	endpoint, err := f.findValidEndpoint()

	if err != nil {
		return nil, err
	}
	urlStr, err := urlStrForEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(urlStr, http.Header{})
	if err != nil {
		putEndpointOnIce(endpoint)
		return f.connectSocket()
	}
	return ws, err
}

func (f *EndpointFactory) findValidEndpoint() (*sync.Map, error) {
	return f.selectEndpoint( nil)
}

func (f *EndpointFactory) selectEndpoint(firstid *uuid.UUID) (*sync.Map, error) {
	// todo : this might require the sync mutex when retrieving from the channel
	f.mu.Lock()
	m := <- f.endpoints
	f.endpoints <- m
	f.mu.Unlock()
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
		if len(f.endpoints) == 1 {
			// if there is only one endpoint in the channel then make sure to return instead of
			// trying recursion
			return nil, EndpointOnIceError
		}

		// if there is only one in the queue then this won't work, and if none are available
		// then this will freeze
		if firstid == nil {
			firstid = eid
		}
		sm, err := f.selectEndpoint(firstid)
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

func (f *EndpointFactory) failedEndpoint(con *pool.PoolConn) {
	con.MarkUnusable()
	val, ex := f.endpointmap.Load(con.RemoteAddr().String())
	if ex {
		sm := val.(*sync.Map)
		putEndpointOnIce(sm)
	}
}

func (f *EndpointFactory) successfulEndpoint(con *pool.PoolConn) {
	val, ex := f.endpointmap.Load(con.RemoteAddr().String())
	if ex {
		sm := val.(*sync.Map)
		reduceErrorScore(sm)
	}
}