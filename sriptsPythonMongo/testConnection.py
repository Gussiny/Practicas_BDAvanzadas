
from pymongo import MongoClient

client = MongoClient('mongodb+srv://ShyGuy118:fg1511973@cluster0-lqqq1.mongodb.net/test?retryWrites=true&w=majority')
db = client.sample_mflix







def printDocs(res):
    i=1
    for doc in res:
        print(i,'-> title: '+str(doc['title'])+'   year: '+str(doc['year'])+'  rating: '+str(doc['imdb']))
        i+=1


def searchBy(yearInp, byRating, top):
    if(byRating):
        print('Best '+str(top)+' movies from '+str(yearInp))
        moviesBy = db.movies.find( {"year":yearInp},{ "id" : 1, "title": 1, 'imdb.rating':1, 'directors':1, 'year':1} ).sort('imdb.rating',-1)[0:top]
    elif(byRating is None):
        print(str(top)+' movies from '+str(yearInp))
        moviesBy = db.movies.find( {"year":yearInp},{ "id" : 1, "title": 1, 'imdb.rating':1, 'directors':1, 'year':1} )[0:top]
    else:
        print('Worst '+str(top)+' movies from '+str(yearInp))
        moviesBy = db.movies.find( {"year":yearInp},{ "id" : 1, "title": 1, 'imdb.rating':1, 'directors':1, 'year':1} ).sort('imdb.rating',1)[0:top]
    
    printDocs(moviesBy)


def topAll(byRating, top):
    if(byRating):
        print(str(top)+ ' Best movies of all time')
        moviesBy = db.movies.find(  { 'year': { '$nin': [''] } ,'imdb.rating': { '$nin': [''] }}).sort('imdb.rating',-1)[0:top]
    elif(byRating is None):
        print(str(top)+ ' movies')
        moviesBy = db.movies.find()[0:top]
    else:
        print(str(top)+ ' Worst movies of all time')
        moviesBy = db.movies.find( { 'year': { '$nin': [''] } ,'imdb.rating': { '$nin': [''] }}).sort('imdb.rating',1)[0:top]

    printDocs(moviesBy)

def searchByTitle(title):
    print('Resulst for: '+str(title))

    moviesBy = db.movies.find({'title': {'$regex' : ".*"+str(title)+".*"} }).sort('year',-1)[0:10]

    printDocs(moviesBy)

def searchByDirector(director):
    print('Resulst for: '+str(director))

    moviesBy = db.movies.find({'directors': {'$regex' : ".*"+str(director)+".*"} })[0:50]

    printDocs(moviesBy)


def searchByDirAndTitle(word):
    print('Resulst for: '+str(word))
    moviesBy = db.movies.find({'directors': {'$regex' : ".*"+str(word)+".*"} , 'title': {'$regex' : ".*"+str(word)+".*"} })[0:50]
    printDocs(moviesBy)




def main():
    #searchBy(1999,True,10)

    #searchBy(2002,False,5)

    #searchBy(2002,None,10)

    #searchBy(2010,None,100)
    


    #topAll(True, 20)

    searchByTitle('Matrix')
    
    
    client.close()
    

main()

