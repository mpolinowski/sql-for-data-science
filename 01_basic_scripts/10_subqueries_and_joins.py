#!/usr/bin/python
import psycopg2
from config import config


def get_penguins_subquery(unit, condition):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT uid, island, species FROM penguins_a WHERE uid IN (SELECT uid FROM penguins_b WHERE {0} {1})".format(unit, condition)
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


def get_penguins_inner_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM penguins_a INNER JOIN penguins_b ON penguins_a.uid = penguins_b.uid".format(column1, column2, column3)
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


def get_penguins_alias_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM penguins_a AS a, penguins_b AS b WHERE a.uid = b.uid".format(column1, column2, column3)
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


def get_penguins_multi_inner_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM ((penguins_a pa INNER JOIN penguins_b pb ON pa.uid = pb.uid) INNER JOIN penguins p ON  pa.uid = p.uid)".format(column1, column2, column3)
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


def get_penguins_self_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM penguins_a AS a, penguins_b AS b WHERE a.uid = b.uid".format(column1, column2, column3)
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


def get_penguins_self_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0} AS Species, {1} AS Island, {2} AS Mass FROM penguins_a a, penguins_b b WHERE a.uid = b.uid AND a.uid = b.uid ORDER BY b.body_mass_g".format(column1, column2, column3)
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


def get_penguins_self_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0} AS Species, {1} AS Island, {2} AS Mass FROM penguins_a a, penguins_b b WHERE a.uid = b.uid AND a.uid = b.uid ORDER BY b.body_mass_g".format(column1, column2, column3)
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


def get_penguins_left_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM penguins_a a LEFT JOIN penguins_b b ON a.uid = b.uid".format(column1, column2, column3)
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


def get_penguins_full_join(column1, column2, column3):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {2} FROM penguins_b b FULL OUTER JOIN penguins_a a ON a.uid = b.uid".format(column1, column2, column3)
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


def get_penguins_concat_strings(column1, column2, filter):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, {0} || ' (' || {1} || ')' FROM penguins_a WHERE sex = '{2}'".format(column1, column2, filter)
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


def get_penguins_sub_strings(column1, column2, filter):
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT {0}, {1}, UPPER(SUBSTR({1},1,3)) FROM penguins_a".format(column1, column2, filter)
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


def get_tweets_datetime_strings():
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT tweet_created, EXTRACT(ISOYEAR FROM tweet_created), EXTRACT(MONTH FROM tweet_created), EXTRACT(Day FROM tweet_created),  EXTRACT(DOW FROM tweet_created) FROM tweets;"
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


def get_penguins_binary_classification():
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT airline, airline_sentiment, CASE airline_sentiment WHEN 'negative' THEN 0 ELSE 1 END negative FROM tweets"
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


def get_penguins_multi_classification():
    """ query data from the penguins table """
    results = []
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        query = "SELECT species, island, sex, CASE WHEN body_mass_g < 3000 THEN 'A' WHEN body_mass_g >= 3001 AND body_mass_g <=6000 THEN 'B' WHEN body_mass_g >= 6000 THEN 'C' ELSE 'X' END mass_class FROM penguins"
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
    # return specimen that weight more than 6kg
    results = get_penguins_subquery(unit='body_mass_g', condition='>6000')
    print(results)
    # return species and island from table A and body mass from table B
    results = get_penguins_inner_join(column1='species', column2='island', column3='body_mass_g')
    print(results)
    # return species and island from table A and body mass from table B
    results = get_penguins_alias_join(column1='species', column2='island', column3='body_mass_g')
    print(results)
    # return species from table A, island from the original table and body mass from table B
    results = get_penguins_multi_inner_join(column1='pa.species', column2='p.island', column3='pb.body_mass_g')
    print(results)
    # return species and island from table A and body mass from table B
    results = get_penguins_self_join(column1='species', column2='island', column3='body_mass_g')
    print(results)
    # return species and island from table A and body mass from table B
    results = get_penguins_self_join(column1='a.species', column2='a.island', column3='b.body_mass_g')
    print(results)
    # return species and island from table A and body mass from table B
    results = get_penguins_self_join(column1='a.species', column2='a.island', column3='b.body_mass_g')
    print(results)
    results = get_penguins_left_join(column1='a.species', column2='a.island', column3='b.body_mass_g')
    print(results)
    results = get_penguins_full_join(column1='a.species', column2='a.island', column3='b.body_mass_g')
    print(results)
    results = get_penguins_concat_strings(column1='species', column2='island', filter='MALE')
    print(results)
    results = get_penguins_sub_strings(column1='species', column2='island')
    print(results)
    results = get_tweets_datetime_strings()
    print(results)
    results = get_penguins_binary_classification()
    print(results)
    results = get_penguins_multi_classification()
    print(results)
    