#!/usr/bin/python
import psycopg2
from config import config


def get_penguins_return_ordered(island, orderby):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT (species, island, sex) FROM penguins WHERE island = '{0}' ORDER BY {1} DESC".format(island, orderby)
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


def get_penguins_return_math(uid):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT (culmen_length_mm + culmen_depth_mm) / body_mass_g AS ident FROM penguins WHERE uid = '{0}'".format(uid)
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


def get_penguins_return_avg(column):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT AVG({0}) AS average FROM penguins".format(column)
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


def get_penguins_return_count(column):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT COUNT({0}) AS row_count FROM penguins".format(column)
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


def get_penguins_return_count_distinct(column):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT COUNT(DISTINCT {0}) AS distinct_row_count FROM penguins".format(column)
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


def get_penguins_return_minmax(column):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT MIN({0}) AS min, MAX({0}) AS max FROM penguins".format(column)
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


def get_penguins_return_sum_total(column1, column2):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        query = "SELECT SUM({0} + {1}) AS total FROM penguins".format(column1, column2)
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


def get_penguins_return_count_groups(column, groups):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT COUNT(DISTINCT {0}) AS distinct_row_count FROM penguins GROUP BY {1}".format(column, groups)
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


def get_penguins_return_group_count_agg(group, condition):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, COUNT(*) AS distinct_row_count FROM penguins GROUP BY {0} HAVING COUNT (*) {1}".format(group, condition)
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
    results = get_penguins_return_ordered(island='Dream', orderby='species')
    print(results)
    results = get_penguins_return_math(uid='649bc5d39b21a1b93e0985a3')
    print(results)
    results = get_penguins_return_avg(column='body_mass_g')
    print(results)
    # count all rows in a table
    results = get_penguins_return_count(column='*')
    print(results)
    # count all rows in specified column excluding NULL values
    results = get_penguins_return_count(column='sex')
    print(results)
    # count all rows in specified column excluding NULL values and duplicates
    results = get_penguins_return_count(column='uid')
    print(results)
    results = get_penguins_return_minmax(column='body_mass_g')
    print(results)
    results = get_penguins_return_sum_total(column1='culmen_length_mm', column2='culmen_depth_mm')
    print(results)
    # this returns 3 groups (MALE, FEMALE, NULL) for all 3 island groups
    results = get_penguins_return_count_groups(column='uid', groups='island, sex')
    print(results)
    # return counts for all species that have more than 100 specimens
    results = get_penguins_return_group_count_agg(group='species', condition='>100')
    print(results)
