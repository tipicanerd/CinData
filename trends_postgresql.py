#!/usr/bin/python3

import requests
import json
from datetime import date
import psycopg2
from psycopg2.errors import UniqueViolation


def InsertSQL(movie):
    global cursor

    #Movies

    genres=movie.pop('genres')

    #JSON atrs
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
    trend_day=str(date.today())

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

def GetAtributes(movie):
    global imgbaseurl,api_key

    id_movie=str(movie['id'])
    url_movie="https://api.themoviedb.org/3/movie/"
    murl=url_movie+id_movie
    margs={'api_key':api_key}

    mdetails=requests.get(murl,params=args)
    movie_det=mdetails.json()

    if movie_det['backdrop_path']!=None:
        movie_det['backdrop_path']=imgbaseurl+movie_det['backdrop_path']
    else:
        movie_det['backdrop_path']=""

    movie_det['poster_path']=imgbaseurl+movie_det['poster_path']
    movie_det['genres']=movie['genre_ids']

    return(movie_det)
            


if __name__ == '__main__':
    
    usr="jlopez"
    psw=open('psw.txt').readlines()[0].replace("\n","")
    #Postgres
    connection = psycopg2.connect(user=usr,password=psw,host="127.0.0.1",port="5432",database=usr)
    cursor = connection.cursor()
    
    print("From JSON to DataBase")
    
    #URLs and complements
    url="https://api.themoviedb.org/3/trending/movie/day?"
    imgbaseurl="https://image.tmdb.org/t/p/original/"
    
    api_key=open('apikey.txt').readlines()[0].replace("\n","")
    
    args={'api_key':api_key}
    response = requests.get(url,params=args)

    
    if response.status_code == 200:
        payload = response.json()
        if payload['total_results']!=0:
            trends=payload['results']

            for m in trends:
                movie=GetAtributes(m)
                InsertSQL(movie)
        
                    
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        print("Done!")
            
   
