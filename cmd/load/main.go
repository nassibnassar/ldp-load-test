package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	_ "github.com/lib/pq"
)

// Loan specifies a loan transaction.
type Loan struct {
	Id       string `json:"id,omitempty"`
	UserId   string `json:"userId,omitempty"`
	ItemId   string `json:"itemId,omitempty"`
	LoanDate string `json:"loanDate,omitempty"`
}

// LoadLoans reads JSON data extracted from mod-circulation-storage:
// /loan-storage/loans.
func LoadLoans(r io.Reader) error {

	dec := json.NewDecoder(r)

	// Skip past first tokens.
	for x := 0; x < 3; x++ {
		_, err := dec.Token()
		if err != nil {
			return err
		}
	}

	// Read array elements.
	for dec.More() {
		var l Loan
		err := dec.Decode(&l)
		if err != nil {
			return err
		}

		//fmt.Printf("%v | %v | %v\n", l.Id, l.UserId, l.LoanDate)
	}

	return nil
}

// LoadLoanFile reads a file containing JSON data extracted from
// mod-circulation-storage: /loan-storage/loans.
func LoadLoanFile(jsonFilename string) error {

	jsonFile, err := os.Open(jsonFilename)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	err = LoadLoans(jsonFile)
	if err != nil {
		return err
	}

	return nil
}

func printError(err error) {
	fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
}

func main() {
	sourceDir := "/Users/nassib/tmp/20181214_043055"

	for x := 1; x <= 20; x++ {
		jsonFilename := sourceDir +
			fmt.Sprintf("/loan-storage.loans.json.%v", x)
		fmt.Printf("%v\n", jsonFilename)

		err := LoadLoanFile(jsonFilename)
		if err != nil {
			printError(err)
			return
		}
	}
}
