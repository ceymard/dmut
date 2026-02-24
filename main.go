// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	_ "github.com/lib/pq"
)

var VERSION = "1.0.0"

/**
dmut collect <outfile.yml> paths...
  collect all paths into a single yaml file
dmut test [options] paths...
  test the mutations on an empty test database that will be created in docker
  options:
	  --image <postgres_image_name> test on a specific postgres image
dmut apply [options] <host> paths...
  apply the mutations to the database
dmut version
  show the version
*/

func main() {

}
