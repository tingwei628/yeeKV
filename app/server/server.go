package server

import (
	"github.com/codecrafters-io/redis-starter-go/app/datastore"
)

type Server struct {
	SafeMap    *datastore.SafeMap
	SafeList   *datastore.SafeList
	SafeStream *datastore.SafeStream
}

func NewServer() *Server {
	return &Server{
		SafeMap:    datastore.NewSafeMap(),
		SafeList:   datastore.NewSafeList(),
		SafeStream: datastore.NewSafeStream(),
	}
}
