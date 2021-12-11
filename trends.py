#!/usr/bin/python3

import requests
import json
from datetime import date



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
    
    print("Movie Trends JSON downloader")
    
    #URLs and complements
    url="https://api.themoviedb.org/3/trending/movie/day?"
    url_movie="https://api.themoviedb.org/3/movie/"
    imgbaseurl="https://image.tmdb.org/t/p/original/"
    
    api_key=open('apikey.txt').readlines()[0].replace("\n","")
    
    args={'api_key':api_key}
    response = requests.get(url,params=args)

    
    if response.status_code == 200:
        movies=[]
        payload = response.json()
        if payload['total_results']!=0:
            trends=payload['results']

            for m in trends:
                movie=GetAtributes(m)
                movies.append(movie)
        

        #Save JSON file
                    
        jname='movie_trends'+str(date.today())+".json"
        jfile=open(jname,'w')
        json.dump(movies, jfile)
            
        jfile.close()
            
        print("Done!")
            
    else:
        print("Movie not found")
