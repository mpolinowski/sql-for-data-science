#!/usr/bin/python
import psycopg2
from config import config


def get_penguins_in(group):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, sex) FROM penguins WHERE uid IN {0}".format(group)
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


def get_penguins_or(island1, island2):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT (species, island, sex) FROM penguins WHERE (island = '{0}' OR island = '{1}')".format(island1, island2)
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


def get_penguins_and(body_mass_g, island):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT (species, island, sex) FROM penguins WHERE body_mass_g >= '{0}' AND island = '{1}'".format(body_mass_g, island)
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


def get_penguins_not(island, sex):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT (species, island, sex) FROM penguins WHERE NOT island = '{0}' AND NOT sex = '{1}'".format(island, sex)
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



def get_penguins_like(island, sex):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT (species, island, sex) FROM penguins WHERE island LIKE '{0}' AND sex LIKE '{1}%'".format(island, sex)
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
    results = get_penguins_in(
        group=('649bc5d39b21a1b93e0985a5', '649bc5d39b21a1b93e0986de', '649bc5d39b21a1b93e098671')
        )
    print(results)
    results = get_penguins_or(island1='Biscoe',island2='Dream')
    print(results)
    results = get_penguins_and(body_mass_g=6000, island='Biscoe')
    print(results)
    results = get_penguins_not(island='Biscoe',sex='MALE')
    print(results)
    results = get_penguins_like(island='%eam',sex='MA%')
    print(results)