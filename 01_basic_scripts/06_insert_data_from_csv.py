#!/usr/bin/python

import pandas as pd
import psycopg2

from config import config

data = pd.read_csv ('dataset_penguins.csv')   
df = pd.DataFrame(data)


def create_table():
    """ delete vendors by id """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cursor = conn.cursor()
        # create the table
        cursor.execute('''
		CREATE TABLE penguins (
			uid CHAR(25) primary key,
			species VARCHAR(10),
            island VARCHAR(10),
			culmen_length_mm FLOAT,
			culmen_depth_mm FLOAT,
			flipper_length_mm FLOAT,
			body_mass_g INT,
			sex VARCHAR(10)
			)
               ''')
        conn.commit()
        # Close communication with the PostgreSQL database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def add_data():
    """ delete vendors by id """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cursor = conn.cursor()
        # insert data
        for row in df.itertuples():
            cursor.execute('INSERT INTO penguins (uid, species, island, culmen_length_mm, culmen_depth_mm, flipper_length_mm, body_mass_g, sex) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (row.uid, row.species, row.island, row.culmen_length_mm, row.culmen_depth_mm, row.flipper_length_mm, row.body_mass_g, row.sex))
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    create_table()
    add_data()