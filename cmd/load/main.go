package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/lib/pq"
)

// Loan specifies a loan transaction.
type Loan struct {
	Id       string `json:"id,omitempty"`
	UserId   string `json:"userId,omitempty"`
	ItemId   string `json:"itemId,omitempty"`
	LoanDate string `json:"loanDate,omitempty"`
}

// LoadLoans reads JSON data extracted from mod-circulation-storage:
// /loan-storage/loans and loads it to the specified LDP foliocore
// database.
func LoadLoans(r io.Reader, db *sql.DB) error {

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
		var l Loan
		err := dec.Decode(&l)
		if err != nil {
			return err
		}

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

// LoadLoanFile reads a file containing JSON data extracted from
// mod-circulation-storage: /loan-storage/loans and loads it to the
// specified LDP foliocore database.
func LoadLoanFile(jsonFilename string, db *sql.DB) error {

	jsonFile, err := os.Open(jsonFilename)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	err = LoadLoans(jsonFile, db)
	if err != nil {
		return err
	}

	return nil
}

// OpenDatabase opens a new database connection.
func OpenDatabase(host, port, user, password, dbname string) (*sql.DB, error) {

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

func main() {
	sourceDir := "/Users/nassib/tmp/20181214_043055"

	db, err := OpenDatabase(
		"localhost", "5432", "okapi", "okapi25", "okapi")
	if err != nil {
		printError(err)
		return
	}
	defer db.Close()

	for x := 1; x <= 20; x++ {
		jsonFilename := sourceDir +
			fmt.Sprintf("/loan-storage.loans.json.%v", x)
		fmt.Printf("%v\n", jsonFilename)

		err := LoadLoanFile(jsonFilename, db)
		if err != nil {
			printError(err)
			return
		}
	}
}
