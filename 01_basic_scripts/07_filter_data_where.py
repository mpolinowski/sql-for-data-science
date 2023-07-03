#!/usr/bin/python

import psycopg2
from config import config


def get_penguins_by_id(uid):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, body_mass_g, sex) FROM penguins WHERE uid = '{0}'".format(uid)
        cursor.execute(query)
        penguins = cursor.fetchall()
        
        for penguin in penguins:
            results.append(penguin)

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return(results)
            conn.close()


def get_penguins_by_mass_thresh(body_mass_g):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, sex) FROM penguins WHERE body_mass_g >= '{0}'".format(body_mass_g)
        cursor.execute(query)
        penguins = cursor.fetchall()
        
        for penguin in penguins:
            results.append(penguin)

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return(results)
            conn.close()


def get_penguins_by_non_match(species):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, sex) FROM penguins WHERE species <> '{0}'".format(species)
        cursor.execute(query)
        penguins = cursor.fetchall()
        
        for penguin in penguins:
            results.append(penguin)

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return(results)
            conn.close()

def get_penguins_by_mass_range(lower, upper):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, sex) FROM penguins WHERE body_mass_g BETWEEN '{0}' AND '{1}'".format(lower, upper)
        cursor.execute(query)
        penguins = cursor.fetchall()
        
        for penguin in penguins:
            results.append(penguin)

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return(results)
            conn.close()

def get_penguins_where_sex_is_null():
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        query = "SELECT (uid, species, island) FROM penguins WHERE sex IS NULL"
        
        cursor.execute(query)
        penguins = cursor.fetchall()
        
        for penguin in penguins:
            results.append(penguin)

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            return(results)
            conn.close()


if __name__ == '__main__':
    results = get_penguins_by_id(uid='649bc5d39b21a1b93e0985a5')
    print(results)
    results = get_penguins_by_mass_thresh(body_mass_g=6000)
    print(results)
    results = get_penguins_by_non_match(species='Adelie')
    print(results)
    results = get_penguins_by_mass_range(lower=4300, upper=4500)
    print(results)
    results = get_penguins_where_sex_is_null()
    print(results)