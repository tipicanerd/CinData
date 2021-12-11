#!/usr/bin/python3

import requests
import json
import psycopg2
from psycopg2.errors import UniqueViolation


def InsertSQL(movie):
    global date,cursor

    #Movies

    genres=movie.pop('genres')

    # JSON atrs
    movie['belongs_to_collection']=json.dumps(movie['belongs_to_collection'])
    movie['production_companies']=json.dumps(movie['production_companies'])
    movie['production_countries']=json.dumps(movie['production_countries'])
    movie['spoken_languages']=json.dumps(movie['spoken_languages'])

    #All atrs
    atrs=','.join(list(movie.keys()))

    values=list(movie.values())

    # Replacing null values for None
    for i in range(len(values)):
        if values[i]=='null':
            values[i]=None

    values=tuple(values)



    query = """ INSERT INTO movies ("""+atrs+""") 
    VALUES %s"""

    #Movies genres relation
    movie_genre = []
    for g in genres:
        tup = movie['id'],str(g)
        movie_genre.append(tup)

    records_list_template = ','.join(['%s'] * len(movie_genre))
    query2 = """INSERT INTO movie_genre(id_movie,id_genre) 
        VALUES {} """.format(records_list_template)

    #Movies trends
    mt_atrs=','.join(['id','popularity','revenue','status',
    'vote_average','vote_count','trend_day'])


    idm = movie['id']
    budget = movie['budget']
    popularity = movie['popularity']
    revenue = movie['revenue']
    status = movie['status']
    vote_average = movie['vote_average']
    vote_count = movie['vote_count']
    trend_day=date

    mt_values=tuple([idm,popularity,revenue,status,vote_average,vote_count,trend_day])

    query3 = """ INSERT INTO movies_trends ("""+mt_atrs+""") 
    VALUES %s"""

    try:
        cursor.execute(query,(values,) )
        cursor.execute(query2,movie_genre)
        cursor.execute(query3,(mt_values,) )

    except UniqueViolation as e:
        #If the movie is already in the database, then update the values

        query = """UPDATE movies 
        SET popularity=%s,
        revenue=%s,
        vote_average=%s,
        vote_count=%s,
        budget=%s,
        status=%s
        WHERE id=%s;
        """
        cursor.execute("ROLLBACK")
        cursor.execute(query,(popularity,revenue,vote_average,vote_count,budget,status,idm,) )
        cursor.execute(query3,(mt_values,))

    connection.commit()
    count = cursor.rowcount
            

if __name__ == '__main__':

    
    #connection details

    usr="jlopez"
    psw=open('psw.txt').readlines()[0].replace("\n","")
    
    #Postgres
    connection = psycopg2.connect(user=usr,password=psw,host="127.0.0.1",port="5432",database=usr)
    cursor = connection.cursor()
    
    print("Recover")
    date=input("Date (yyyy-mm-dd):  ")
    trends = open('movie_trends'+date+'.json')
    trends = json.load(trends)
    

    for movie in trends:
        InsertSQL(movie)
        
                    
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
            
   
