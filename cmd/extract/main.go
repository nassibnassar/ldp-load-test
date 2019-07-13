package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

const okapiUrl = "http://ec2-18-232-163-248.compute-1.amazonaws.com:9130"

//const okapiUrl = "http://localhost:9130"

func main() {
	fmt.Printf("Okapi URL: %s\n", okapiUrl)
	token, err := login()
	if err != nil {
		printError(err)
		return
	}
	err = retrieveLoans(token)
	if err != nil {
		printError(err)
		return
	}
}

func login() (string, error) {
	client := &http.Client{}

	m := map[string]string{
		"username": "diku_admin",
		//"username": "admin",
		"password": "admin",
	}
	json, err := json.Marshal(m)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", okapiUrl+"/authn/login",
		bytes.NewBuffer(json))
	if err != nil {
		return "", err
	}

	req.Header.Add("X-Okapi-Tenant", "diku")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json,text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	return resp.Header["X-Okapi-Token"][0], nil
}

func retrieveLoans(token string) error {
	client := &http.Client{}

	req, err := http.NewRequest("GET",
		okapiUrl+"/loan-storage/loans?limit=100&offset=0",
		//okapiUrl+"/bl-users/by-id/200?limit=100&offset=0",
		nil)
	if err != nil {
		return err
	}

	req.Header.Add("X-Okapi-Tenant", "diku")
	req.Header.Add("X-Okapi-Token", token)
	req.Header.Add("Accept", "application/json,text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", body)

	return nil
}

func printError(err error) {
	fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
}
