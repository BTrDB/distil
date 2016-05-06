package distil

import (
	"fmt"
	"os"
	"time"

	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
	"github.com/pborman/uuid"
	"gopkg.in/mgo.v2"
)

const SPACING float64 = 1e9 / 120

func sliceToChan(s []btrdb.StandardValue) chan btrdb.StandardValue {
	var rv chan btrdb.StandardValue = make(chan btrdb.StandardValue)
	
	go func () {
		for sv := range s {
			rv <- sv
		}
		close(rv)
	}()
	
	return rv
}

func chanToSlice(svs chan btrdb.StandardValue) []btrdb.StandardValue {
	var rv = make([]btrdb.StandardValue)
	for sv := range svs {
		rv = append(rv, sv)
	}
	return rv
}
