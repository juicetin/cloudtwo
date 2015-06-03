# Calculate the average rating of each genre
# Note: this is a python-spark implementation of the java-spark code shown in the lecture (MovieLensLarge)
# In order to run this, we use spark-submit, but with a different argument
# spark-submit  \
#   --master yarn-cluster \
#   --num-executors 3 \
#   AverageRatingPerGenre.py

from pyspark import SparkContext

# This function convert entries of movies.csv into key,value pair of the following format
# movieID -> genre
# since there may be multiple genre per movie, this function returns a list of key,value pair
def pairMovieToGenre(record):
  try:
    movieID, name, genreList = record.split(",")
    genres = genreList.split("|")
    return [(movieID, genre) for genre in genres]
  except:
    return []

# This function convert entries of ratings.csv into key,value pair of the following format
# movieID -> rating
def extractRating(record):
  try:
    userID, movieID, rating, timestamp = record.split(",")
    rating = float(rating)
    return (movieID, rating)
  except:
    return ()

# This functions convert tuples of (genre, rating) into key,value pair of the following format
# genre -> rating
def mapToPair(line):
  genre, rating = line
  return (genre, rating)

# This function is used by the aggregateByKey function to merge rating of the same keys in the same combiner
# This function takes in either the starting value (0.0,0) or the result of previous call to mergeRating function 
# in the form of (total rating, number of rating), and the value currently being 'processed'.
def mergeRating(accumulatedPair, currentRating):
  ratingTotal, ratingCount = accumulatedPair
  ratingTotal += currentRating
  ratingCount += 1
  return (ratingTotal, ratingCount)

# This function is used by the aggregateByKey function to merge rating of the same keys from different combiner
# This function takes in the result of mergeRating function from different combiners (in the form of (total rating, number of rating))
def mergeCombiners(accumulatedPair1, accumulatedPair2):
  ratingTotal1, ratingCount1 = accumulatedPair1
  ratingTotal2, ratingCount2 = accumulatedPair2
  return (ratingTotal1+ratingTotal2, ratingCount1+ratingCount2)

# This function takes the statistic of ratings per genre and calculate the average rating
def mapAverageRating(line):
  genre, ratingTotalCount = line
  ratingAverage = ratingTotalCount[0]/ratingTotalCount[1]
  return (genre, genreRatingsAverage)

# This is line is important for submitting perython jobs through spark-submit!
# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
# Thus the code under the if statement will only be evaluated when the script is called from command line
if __name__ == "__main__":
  sc = SparkContext(appName="Average Rating per Genre")
  ratings = sc.textFile("/share/ml/latest/ratings.csv")
  movieData = sc.textFile("/share/ml/latest/movies.csv")

  movieRatings = ratings.map(extractRating)
  # Movie ID, Movie Rating
  # (u'6839', 2.5)
  # (u'6863', 3.0)
  # (u'6874', 4.0)
  # (u'6881', 2.5)
  # (u'6888', 1.0)
  # (u'6942', 3.0)

  movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
  # Movie ID, Genre
  # (u'69768', u'Drama')
  # (u'69768', u'Romance')
  # (u'69771', u'Drama')
  # (u'69771', u'Romance')
  # (u'69773', u'Drama')
  # (u'69773', u'Romance')
  # (u'69784', u'Comedy')
  # (u'69788', u'Documentary')

  genreRatings = movieGenre.join(movieRatings).values().map(mapToPair)
  # Genre, Rating   (per movie - heaps of repeats of these keys)
  # (u'Sci-Fi', 4.0)
  # (u'Sci-Fi', 3.0)
  # (u'Sci-Fi', 2.0)
  # (u'Sci-Fi', 2.0)
  # (u'Sci-Fi', 2.5)
  # (u'Sci-Fi', 3.0)

  genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), mergeRating, mergeCombiners, 1).map(mapAverageRating)
  genreRatingsAverage.saveAsTextFile("pySparkRatingPerGenre")
  