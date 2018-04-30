package twitter

import (
	"github.com/ChimeraCoder/anaconda"
	log "github.com/sirupsen/logrus"
)

// Demux receives channels or interfaces and type switches them to call the appropriate handle function.
type Demux interface {
	Handle(m interface{})
	HandleChan(c <-chan interface{})
}

// StreamDemux receives messages and type switches them to call functions with typed messages.
type StreamDemux struct {
	All        func(m interface{})
	Tweet      func(tweet anaconda.Tweet)
	Event      func(event anaconda.Event)
	EventTweet func(event anaconda.EventTweet)
	Other      func(m interface{})
}

// NewTwitterAPI creates a new anaconda instance.
// Anaconda is a Twitter API Drivers (github.com/ChimeraCoder/anaconda).
func NewTwitterAPI(consumerkey string, consumersecret string, accesstoken string, accesstokensecret string) *anaconda.TwitterApi {
	anaconda.SetConsumerKey(consumerkey)
	anaconda.SetConsumerSecret(consumersecret)
	api := anaconda.NewTwitterApi(accesstoken, accesstokensecret)
	if _, err := api.VerifyCredentials(); err != nil {
		log.Fatalf("[SVC-LISTEN] Bad Authorization Tokens. Please refer to https://apps.twitter.com/ for your Access Tokens: %s", err)
	}
	return api
}

// NewStreamDemux initializes a new StreamDemux.
func NewStreamDemux() StreamDemux {
	return StreamDemux{
		All:        func(m interface{}) {},
		Tweet:      func(tweet anaconda.Tweet) {},
		Event:      func(event anaconda.Event) {},
		EventTweet: func(event anaconda.EventTweet) {},
		Other:      func(m interface{}) {},
	}
}

// Handle handles messages.
func (d StreamDemux) Handle(m interface{}) {
	d.All(m)

	switch t := m.(type) {
	case anaconda.Tweet:
		d.Tweet(t)
	case anaconda.Event:
		d.Event(t)
	case anaconda.EventTweet:
		d.EventTweet(t)
	default:
		d.Other(t)
	}
}

// HandleChan handles channels.
func (d StreamDemux) HandleChan(c <-chan interface{}) {
	for m := range c {
		d.Handle(m)
	}
}
