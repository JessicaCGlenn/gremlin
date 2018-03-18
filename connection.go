package gremlin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"github.com/gorilla/websocket"
	"github.com/jessicacglenn/pool"
)

// Clients include the necessary info to connect to the server and the underlying socket
type Client struct {
	Remote 		*url.URL
	pool		pool.Pool
	Auth   		[]OptAuth
	factory		*EndpointFactory
}



var (
	MalformedClusterStringErr = errors.New("connection string is not in expected format. An example of the expected format is 'ws://server1:8182, ws://server2:8182'")
	UserNotSetErr = errors.New("variable GREMLIN_USER is not set")
	PassNotSetErr = errors.New("variable GREMLIN_PASS is not set")
	UnknownErr = errors.New("an unknown error occurred")
	NoEndpointsError = errors.New("no valid endpoints provided")
	NoServersSetError = errors.New("no servers set, configure servers to connect to using the GREMLIN_SERVERS environment variable")
)

// perhaps if we change the urlStr to interface{} we can have either a slice or a string passed through and
// we will be able to use this for both the
func NewClient(urlStr string, options ...OptAuth) (*Client, error) {

	fact, err := NewEndpointFactory(urlStr)
	if err != nil {
		return nil, err
	}

	c := &Client{Auth: options, factory: fact}

	p, err := pool.NewChannelPool(1, 30, fact.connectSocket)
	if err != nil {
		return nil, err
	}
	c.pool = p
	return c, nil
}




// if the server is not reachable then we should mark it as unavailable
// and then try again a little later.

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
	b, err := c.readResponse(con)

	// update the endpoint to mark success/error, this allows us to back off endpoints that are continuing to fail
	if err != nil {
		c.factory.failedEndpoint(con)
	} else {
		c.factory.successfulEndpoint(con)
	}

	// if the request was successful, return to the pool, otherwise close and remove from the pool.
	err = con.Close()
	return b, err
}


// this doesn't seem to be useful outside of the Exec function (in this context)
func (c *Client) readResponse(con *pool.PoolConn) (data []byte, err error) {
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

			return

		case StatusAuthenticate:

			return c.authenticate(con, res.RequestId)
		case StatusPartialContent:
			inBatchMode = true
			if err = json.Unmarshal(res.Result.Data, &items); err != nil {
				return
			}
			dataItems = append(dataItems, items...)


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

			return

		default:
			if errmsg, exists := ConnectionErrors[res.Status.Code]; exists {
				err = errmsg
			} else {
				err = UnknownErr
			}
			return
		}
	}
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


// LEGACY

var defaultClient *Client

func NewCluster(s ...string) (err error) {
	var connString string
	// If no arguments use environment variable
	if len(s) == 0 {
		connString = strings.TrimSpace(os.Getenv("GREMLIN_SERVERS"))
	} else {
		connString = strings.Join(s, ",")
	}
	if connString == "" {
		err = NoServersSetError
		return
	}
	defaultClient, err = NewClient(connString)
	return
}
