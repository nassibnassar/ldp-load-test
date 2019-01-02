import json
import os
import psycopg2

def load_loans_insert(json_file, conn):
    js = json.load(json_file)
    with conn.cursor() as cur:
        for loan in js['loans']:
            cur.execute("INSERT INTO loans VALUES (%s, %s, %s)",
                    (loan['id'], loan['userId'], loan['loanDate']))
            conn.commit()

def load_loans_file(json_filename, conn):
    with open(json_filename) as json_file:
            load_loans_insert(json_file, conn)

if __name__ == '__main__':
    source_dir = '/Users/nassib/tmp/20181214_043055'
    with psycopg2.connect('host=localhost port=5432 user=okapi ' +
            'password=okapi25 dbname=ldp_okapi') as conn:
        for x in range(1, 21):
            json_filename = source_dir + '/loan-storage.loans.json.' + str(x)
            print(json_filename)
            # load_loans_file(json_filename, conn)

