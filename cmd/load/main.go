package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/lib/pq"
)

func main() {

	db, err := openDatabase(
		"localhost", "5432", "okapi", "okapi25", "okapi")
	if err != nil {
		printError(err)
		return
	}
	defer db.Close()

	err = loadLoanStdin(db)
	if err != nil {
		printError(err)
		return
	}
}

func openDatabase(host, port, user, password, dbname string) (*sql.DB, error) {

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s "+
			"sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Ping the database to test for connection errors.
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func printError(err error) {
	fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
}

func loadLoanStdin(db *sql.DB) error {

	err := loadLoansCopy(os.Stdin, db)
	if err != nil {
		return err
	}

	return nil
}

func loadLoanFile(jsonFilename string, db *sql.DB) error {

	jsonFile, err := os.Open(jsonFilename)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	err = loadLoansCopy(jsonFile, db)
	if err != nil {
		return err
	}

	return nil
}

func loadLoansCopy(r io.Reader, db *sql.DB) error {

	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(
		"public", "loans",
		"id", "user_id", "loan_date"))
	if err != nil {
		return err
	}

	dec := json.NewDecoder(r)

	// Skip past first tokens.
	for x := 0; x < 3; x++ {
		_, err := dec.Token()
		if err != nil {
			return err
		}
	}

	// Read and load array elements.
	for dec.More() {
		var l loan
		err := dec.Decode(&l)
		if err != nil {
			return err
		}

		fmt.Printf("%v %v %v\n", l.Id, l.UserId, l.LoanDate)
		_, err = stmt.Exec(l.Id, l.UserId, l.LoanDate)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

type loan struct {
	Id       string `json:"id,omitempty"`
	UserId   string `json:"userId,omitempty"`
	ItemId   string `json:"itemId,omitempty"`
	LoanDate string `json:"loanDate,omitempty"`
}
