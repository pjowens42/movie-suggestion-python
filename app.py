import numpy as np
import pandas as pd
from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse, abort
from flask_sqlalchemy import SQLAlchemy 
from flask_marshmallow import Marshmallow
from flask_cors import CORS, cross_origin 
import json
import os

movies_df = pd.read_csv('movies.csv')
ratings_df = pd.read_csv('ratings.csv')

# Init app
app = Flask(__name__)
CORS(app, support_credentials=True)
basedir = os.path.abspath(os.path.dirname(__file__))
# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)

# review Class/Model
class Review(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  userid = db.Column(db.Integer)
  title = db.Column(db.String(168))
  rating = db.Column(db.Float)

# review Schema
class ReviewSchema(ma.SQLAlchemySchema):
    class Meta:
        model = Review

    id = ma.auto_field()
    userid = ma.auto_field()
    title = ma.auto_field()
    rating = ma.auto_field()

db.create_all()
review_schema = ReviewSchema()
reviews_schema = ReviewSchema(many=True)

api = Api(app)

review_put_args = reqparse.RequestParser()
review_put_args.add_argument("title", type=str, help="Name of the movie", required=True)
review_put_args.add_argument("rating", type=float, help="Rating of the movie", required=True)


class TopMovies(Resource):
    def get(self, userid):
        reviews = Review.query.filter_by(userid = userid)
        a_json = reviews_schema.dump(reviews)
        reviews = pd.json_normalize(a_json)
        reviews = reviews.drop('id', 1)
        reviews = reviews.drop('userid', 1)
        reviews = reviews.reindex(columns=['title','rating'])
        reviews = pd.DataFrame(reviews)
        print(reviews)
        output = recommendation_system(reviews)
        return {"movies": [
            {"moviesId": str(output.iloc[0,0]), "title": output.iloc[0,1]},
            {"moviesId": str(output.iloc[1,0]), "title": output.iloc[1,1]},
            {"moviesId": str(output.iloc[2,0]), "title": output.iloc[2,1]},
            {"moviesId": str(output.iloc[3,0]), "title": output.iloc[3,1]},
            {"moviesId": str(output.iloc[4,0]), "title": output.iloc[4,1]},
            {"moviesId": str(output.iloc[5,0]), "title": output.iloc[5,1]},
            {"moviesId": str(output.iloc[6,0]), "title": output.iloc[6,1]},
            {"moviesId": str(output.iloc[7,0]), "title": output.iloc[7,1]},
            {"moviesId": str(output.iloc[8,0]), "title": output.iloc[8,1]},
            {"moviesId": str(output.iloc[9,0]), "title": output.iloc[9,1]},
            ]}

api.add_resource(TopMovies, "/topmovies/<int:userid>")

@app.route('/review', methods=['POST'])
def add_review():
    data = request.get_json(force=True)

    new_review = Review(userid=data['userid'], title=data['title'], rating=data['rating'])

    db.session.add(new_review)
    db.session.commit()
    

# Get All reviews
@app.route('/review', methods=['GET'])
def get_reviews():
  all_reviews = Review.query.all()
  result = reviews_schema.dump(all_reviews)
  return json.dumps(reviews_schema.dump(all_reviews))

# Get reviews
@app.route('/review/<id>', methods=['GET'])
def get_reviews_userid(id):
    reviews = Review.query.filter_by(userid = id)
    return json.dumps(reviews_schema.dump(reviews))

# Delete Product
@app.route('/review/<id>', methods=['DELETE'])
def delete_product(id):
  review = Review.query.get(id)
  db.session.delete(review)
  db.session.commit()

  return { "Delete": str(id)}

#Removing the years from the 'title' column
movies_df['title'] = movies_df.title.str.replace('(\(\d\d\d\d\))', '')

#Applying the strip function to get rid of any ending whitespace characters that may have appeared
movies_df['title'] = movies_df['title'].apply(lambda x: x.strip())
movies_df = movies_df.drop('genres', 1)

ratings_df = ratings_df.drop('timestamp', 1)
 
userInput = [
            {'title':'Toy Story', 'rating':4.5},
            {'title':'Jumanji', 'rating':4.5},
            {'title':'Back to the Future', 'rating':5},
            {'title':'Back to the Future Part II', 'rating':4},
            {'title':'Back to the Future Part III', 'rating':3},
            {'title':'Mad Max Beyond Thunderdome', 'rating':4},
            {'title':'Hook', 'rating':4},
            {'title':'Ghostbusters (a.k.a. Ghost Busters)', 'rating':4.5},
            {'title':'Ghostbusters II', 'rating':3},
            {'title':'2001: A Space Odyssey', 'rating':5},
            {'title':'Who Framed Roger Rabbit?', 'rating':5},
            {'title':'Saving Private Ryan', 'rating':4},
            {'title':'Stargate', 'rating':4},
            {'title':'Goofy Movie', 'rating':5},
            {'title':'Extremely Goofy Movie, An', 'rating':2.5},
            {'title':'Guardians of the Galaxy', 'rating':5},
            {'title':'Guardians of the Galaxy 2', 'rating':5},
            {'title':'My Flesh and Blood', 'rating':4.5},
            {'title':'La La Land', 'rating':1},
            {'title':'Lion King, The', 'rating':4.5}
         ]

#inputMovies = pd.DataFrame(userInput)
def recommendation_system(inputMovies):
    #Filtering out the movies by title
    inputId = movies_df[movies_df['title'].isin(inputMovies['title'].tolist())]

    #Then merging it so we can get the movieId. It's implicitly merging it by title.
    inputMovies = pd.merge(inputId, inputMovies)

    #Filtering out users that have watched movies that the input has watched and storing it
    userSubset = ratings_df[ratings_df['movieId'].isin(inputMovies['movieId'].tolist())]

    #Groupby creates several sub dataframes where they all have the same value in the column specified as the parameter
    userSubsetGroup = userSubset.groupby(['userId'])

    #Sorting it so users with movie most in common with the input will have priority
    userSubsetGroup = sorted(userSubsetGroup,  key=lambda x: len(x[1]), reverse=True)

    userSubsetGroup = userSubsetGroup[0:100]

    #Store the Pearson Correlation in a dictionary, where the key is the user Id and the value is the coefficient
    pearsonCorrelationDict = {}

    #For every user group in our subset
    for name, group in userSubsetGroup:
        
        #Let's start by sorting the input and current user group so the values aren't mixed up later on
        group = group.sort_values(by='movieId')
        inputMovies = inputMovies.sort_values(by='movieId')
        
        #Get the N (total similar movies watched) for the formula 
        nRatings = len(group)
        
        #Get the review scores for the movies that they both have in common
        temp_df = inputMovies[inputMovies['movieId'].isin(group['movieId'].tolist())]
        
        #And then store them in a temporary buffer variable in a list format to facilitate future calculations
        tempRatingList = temp_df['rating'].tolist()
        
        #Let's also put the current user group reviews in a list format
        tempGroupList = group['rating'].tolist()
        
        #Now let's calculate the pearson correlation between two users, so called, x and y

        #For hard code based
        Sxx = sum([i**2 for i in tempRatingList]) - pow(sum(tempRatingList),2)/float(nRatings)
        Syy = sum([i**2 for i in tempGroupList]) - pow(sum(tempGroupList),2)/float(nRatings)
        Sxy = sum( i*j for i, j in zip(tempRatingList, tempGroupList)) - sum(tempRatingList)*sum(tempGroupList)/float(nRatings)
        
        #If the denominator is different than zero, then divide, else, 0 correlation.
        if Sxx != 0 and Syy != 0:
            pearsonCorrelationDict[name] = Sxy/np.sqrt(Sxx*Syy)
        else:
            pearsonCorrelationDict[name] = 0

    pearsonDF = pd.DataFrame.from_dict(pearsonCorrelationDict, orient='index')
    pearsonDF.columns = ['similarityIndex']
    pearsonDF['userId'] = pearsonDF.index
    pearsonDF.index = range(len(pearsonDF))

    topUsers=pearsonDF.sort_values(by='similarityIndex', ascending=False)[0:50]

    topUsersRating = topUsers.merge(ratings_df, left_on='userId', right_on='userId', how='inner')

    #Multiplies the similarity by the user's ratings
    topUsersRating['weightedRating'] = topUsersRating['similarityIndex']*topUsersRating['rating']

    #Applies a sum to the topUsers after grouping it up by userId
    tempTopUsersRating = topUsersRating.groupby('movieId').sum()[['similarityIndex','weightedRating']]
    tempTopUsersRating.columns = ['sum_similarityIndex','sum_weightedRating']

    #Creates an empty dataframe
    recommendation_df = pd.DataFrame()
    #Now we take the weighted average
    recommendation_df['weighted average recommendation score'] = tempTopUsersRating['sum_weightedRating']/tempTopUsersRating['sum_similarityIndex']
    recommendation_df['movieId'] = tempTopUsersRating.index
    recommendation_df = recommendation_df.sort_values(by='weighted average recommendation score', ascending=False)

    return movies_df.loc[movies_df['movieId'].isin(recommendation_df.head(10)['movieId'].tolist())]

# Run Server
if __name__ == "__main__":
    app.run(debug=True)