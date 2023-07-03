#!/usr/bin/python

import psycopg2
from config import config


def get_vendor():
    """ query data from the vendors table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
        print("The number of vendors: ", cursor.rowcount)

        print(cursor.fetchone())

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_all_vendors():
    """ query data from the vendors table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
        rows = cursor.fetchall()
        print("The number of vendors: ", cursor.rowcount)
        for row in rows:
            print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_vendors(size):
    """ query data from the vendors table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
        rows = cursor.fetchmany(size=size)
        print("The number of vendors: ", cursor.rowcount)
        for row in rows:
            print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    # Update vendor id 1
    get_vendor() 
    get_all_vendors()
    get_vendors(size=3)