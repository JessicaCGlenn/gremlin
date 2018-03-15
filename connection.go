package gremlin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jessicacglenn/pool"

	"sync"
)

// Clients include the necessary info to connect to the server and the underlying socket
type Client struct {
	Remote 		*url.URL
	servchan	chan *sync.Map
	pool		pool.Pool
	endpointmap	*sync.Map
	mu			*sync.RWMutex
	Auth   		[]OptAuth

}



var (
	MalformedClusterStringErr = errors.New("connection string is not in expected format. An example of the expected format is 'ws://server1:8182, ws://server2:8182'")
	UserNotSetErr = errors.New("variable GREMLIN_USER is not set")
	PassNotSetErr = errors.New("variable GREMLIN_PASS is not set")
	UnknownErr = errors.New("an unknown error occurred")
	NoEndpointsError = errors.New("no valid endpoints provided")
)

// perhaps if we change the urlStr to interface{} we can have either a slice or a string passed through and
// we will be able to use this for both the
func NewClient(urlStr string, options ...OptAuth) (*Client, error) {

	em, ec, err := newEndpointsChannel(urlStr)
	if err != nil {
		return nil, err
	}

	c := &Client{Auth: options, endpointmap: em, servchan: ec, mu: &sync.RWMutex{}}

	factory    := func() (*websocket.Conn, error) { return c.connectSocket(ec) }
	p, err := pool.NewChannelPool(1, 30, factory)
	if err != nil {
		return nil, err
	}
	c.pool = p
	return c, nil
}




// if the server is not reachable then we should mark it as unavailable
// and then try again a little later.

func (c *Client) connectSocket(ch chan *sync.Map) (*websocket.Conn, error) {
	// lock the client while finding the endpoint
	//c.mu.Lock()
	endpoint, err := findValidEndpoint(ch, c.mu)
	//c.mu.Unlock()

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
		return c.connectSocket(ch)
	}
	return ws, err
}


func (c *Client) Close() {
	c.pool.Close()
}

// Client executes the provided request
func (c *Client) ExecQuery(query string) ([]byte, error) {
	req := Query(query)
	return c.Exec(req)
}

func (c *Client) Exec(req *Request) ([]byte, error) {
	con, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	return c.executeForConn(req, con)
}

func (c *Client) executeForConn(req *Request, con *pool.PoolConn) ([]byte, error) {
	requestMessage, err := GraphSONSerializer(req)
	if err != nil {
		return nil, err
	}

	if err := con.WriteMessage(websocket.BinaryMessage, requestMessage); err != nil {
		print("error", err)
		return nil, err
	}
	return c.ReadResponse(con)
}

func (c *Client) ReadResponse(con *pool.PoolConn) (data []byte, err error) {
	// Data buffer
	var message []byte
	var dataItems []json.RawMessage
	inBatchMode := false
	// Receive data
	for {
		if _, message, err = con.ReadMessage(); err != nil {
			return
		}
		var res *Response
		if err = json.Unmarshal(message, &res); err != nil {
			return
		}
		var items []json.RawMessage
		switch res.Status.Code {
		case StatusNoContent:
			// close the connection and put it back to the pool
			con.Close()
			return

		case StatusAuthenticate:
			// close the connection and put it back to the pool
			con.Close()
			return c.authenticate(con, res.RequestId)
		case StatusPartialContent:
			inBatchMode = true
			if err = json.Unmarshal(res.Result.Data, &items); err != nil {
				return
			}
			dataItems = append(dataItems, items...)
			// close the connection and put it back to the pool
			con.Close()

		case StatusSuccess:
			if inBatchMode {
				if err = json.Unmarshal(res.Result.Data, &items); err != nil {
					return
				}
				dataItems = append(dataItems, items...)
				data, err = json.Marshal(dataItems)
			} else {
				data = res.Result.Data
			}
			val, ex := c.endpointmap.Load(con.RemoteAddr().String())
			if ex {
				sm := val.(*sync.Map)
				reduceErrorScore(sm)
			}

			// close the connection and put it back to the pool
			con.Close()
			return

		default:
			if errmsg, exists := ConnectionErrors[res.Status.Code]; exists {
				err = errmsg
			} else {
				err = UnknownErr
			}

			// mark the connection as unusable so that the pool has to spin a new one up
			con.MarkUnusable()
			con.Close()

			val, ex := c.endpointmap.Load(con.RemoteAddr().String())
			if ex {
				sm := val.(*sync.Map)
				putEndpointOnIce(sm)
			}
			return
		}
	}
	// close the connection and put it back to the pool
	con.Close()
	return
}


// AuthInfo includes all info related with SASL authentication with the Gremlin server
// ChallengeId is the  requestID in the 407 status (AUTHENTICATE) response given by the server.
// We have to send an authentication request with that same RequestID in order to solve the challenge.
type AuthInfo struct {
	ChallengeId string
	User        string
	Pass        string
}

type OptAuth func(*AuthInfo) error

// Constructor for different authentication possibilities
func NewAuthInfo(options ...OptAuth) (*AuthInfo, error) {
	auth := &AuthInfo{}
	for _, op := range options {
		err := op(auth)
		if err != nil {
			return nil, err
		}
	}
	return auth, nil
}

// Sets authentication info from environment variables GREMLIN_USER and GREMLIN_PASS
func OptAuthEnv() OptAuth {
	return func(auth *AuthInfo) error {
		user, ok := os.LookupEnv("GREMLIN_USER")
		if !ok {
			return UserNotSetErr
		}
		pass, ok := os.LookupEnv("GREMLIN_PASS")
		if !ok {
			return PassNotSetErr
		}
		auth.User = user
		auth.Pass = pass
		return nil
	}
}

// Sets authentication information from username and password
func OptAuthUserPass(user, pass string) OptAuth {
	return func(auth *AuthInfo) error {
		auth.User = user
		auth.Pass = pass
		return nil
	}
}


func (c *Client) authenticate(con *pool.PoolConn, requestId string) ([]byte, error) {
	auth, err := NewAuthInfo(c.Auth...)
	if err != nil {
		return nil, err
	}
	var sasl []byte
	sasl = append(sasl, 0)
	sasl = append(sasl, []byte(auth.User)...)
	sasl = append(sasl, 0)
	sasl = append(sasl, []byte(auth.Pass)...)
	saslEnc := base64.StdEncoding.EncodeToString(sasl)
	args := &RequestArgs{Sasl: saslEnc}
	authReq := &Request{
		RequestId: requestId,
		Processor: "trasversal",
		Op:        "authentication",
		Args:      args,
	}
	return c.executeForConn(authReq, con)
}


// todo : find out if this is something we can remove altogether or if we need to be able
// to support the un-load-balanced gremlin servers

var servers []*url.URL

func NewCluster(s ...string) (err error) {
	servers = nil
	// If no arguments use environment variable
	if len(s) == 0 {
		connString := strings.TrimSpace(os.Getenv("GREMLIN_SERVERS"))
		if connString == "" {
			err = errors.New("No servers set. Configure servers to connect to using the GREMLIN_SERVERS environment variable.")
			return
		}
		servers, err = SplitServers(connString)
		return
	}
	// Else use the supplied servers
	for _, v := range s {
		var u *url.URL
		if u, err = url.Parse(v); err != nil {
			return
		}
		servers = append(servers, u)
	}
	return
}



func CreateConnection() (conn net.Conn, server *url.URL, err error) {
	connEstablished := false
	for _, s := range servers {
		c, err := net.DialTimeout("tcp", s.Host, 1*time.Second)
		if err != nil {
			continue
		}
		connEstablished = true
		conn = c
		server = s
		break
	}
	if !connEstablished {
		err = errors.New("Could not establish connection. Please check your connection string and ensure at least one server is up.")
	}
	return
}
