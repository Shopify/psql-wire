package main

import (
	"context"
	"errors"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	if err := wire.ListenAndServe("127.0.0.1:5432", handler); err != nil {
		log.Fatal(err)
	}
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	log.Println("incoming SQL query:", query)

	err := errors.New("unimplemented feature")
	err = psqlerr.WithCode(err, codes.FeatureNotSupported)
	err = psqlerr.WithSeverity(err, psqlerr.LevelFatal)

	return nil, err
}
