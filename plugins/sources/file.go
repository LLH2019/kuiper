package main

import "github.com/emqx/kuiper/xstream/api"

type fileSource struct {

}

func (fs *fileSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {


}

func (fs *fileSource) Configure(datasource string, props map[string]interface{}) error {
	return nil
}