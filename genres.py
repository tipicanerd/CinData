#!/usr/bin/python3

import requests
import json
import psycopg2
from psycopg2.errors import UniqueViolation


if __name__ == '__main__':
    usr="jlopez"
    psw=open('psw.txt').readlines()[0].replace("\n","")
    #Postgres
    connection = psycopg2.connect(user=usr,password=psw,host="127.0.0.1",port="5432",database=usr)
    cursor = connection.cursor()
	#URLs and complements
    url="https://api.themoviedb.org/3/genre/movie/list?"
    
    api_key=open('apikey.txt').readlines()[0].replace("\n","")
    
    args={'api_key':api_key}
    response = requests.get(url,params=args)

    
    if response.status_code == 200:
        payload=response.json()
        glist=payload['genres']
        for g in glist:
            idg=g['id']
            nameg=g['name']

            query = """ INSERT INTO genres(id,name) VALUES (%s, %s)"""

            cursor.execute(query,(idg,nameg) )
            connection.commit()
            count = cursor.rowcount

        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")