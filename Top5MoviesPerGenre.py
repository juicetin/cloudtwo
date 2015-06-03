# Calculate the top 5 movies per genre with aggregator
# Note: this is a python-spark implementation of the java-spark code from the tutorial question (MLGenreTopMoviesByRatingCount.java)
# In order to run this, we use spark-submit, but with a different argument
# spark-submit  \
#   --master yarn-cluster \
#   --num-executors 3 \
#   Top5MoviesPerGenre.py

from pyspark import SparkContext

# This function convert entries of movies.csv into key,value pair of the following format
# movieID -> genre
# since there may be multiple genre per movie, this function returns a list of key,value pair
def pairMovieToGenre(record):
  try:
    movieID, title, genreList = record.strip().split(",")
    genres = genreList.strip().split("|")
    return [(movieID, (title, genre.strip())) for genre in genres]
  except:
    return []

# This function convert entries of ratings.csv into key,value pair of the following format
# movieID -> 1
def extractRating(record):
  try:
    userID, movieID, rating, timestamp = record.strip().split(",")
    return (movieID.strip(), 1)
  except:
    return ()

# This function is used by reduceByKey function to merge count of the same key
# This functions takes in two values - merged count from previous call of sumRatingCount, and the currently processed count
def sumRatingCount(reducedCount, currentCount):
  return reducedCount+currentCount

# This functions convert tuples of ((title, genre), number of rating) into key,value pair of the following format
# genre -> (title, number of rating)
def mapToPair(record):
  titleGenre, count = record
  title, genre = titleGenre
  return (genre, (title, count))

# This function is used by the aggregateByKey function to get the top 5 movies as the movies,count pair is being merged in the same combiner
# This function takes in either the starting value [] or the result of previous call to mergeMaxMovie function (previous top 5 movies) and
# The current movie, count pair.
def mergeMaxMovie(topMovieList, currentMovieCount):
  # Sorted function takes in an iterator (such as list) and returns back a sorted list.
  # The key argument is an optional argument used to choose custom key for sorting.
  # In this case, since the key is the number of rating, we needed to use the lambda function to 'present' the number of rating
  # as key rather than the title.
  topList = sorted(topMovieList+[currentMovieCount], key=lambda rec:rec[-1], reverse=True)
  return topList[:5]

# This function is used by the aggregateByKey function to get the top 5 movies as the top 5 movies list is being merged from different combiner
# This function takes in the result of mergeMaxMovie function from different combiners (in the form of list of top 5 movies)
def mergeCombiners(topMovieList1, topMovieList2):
  topList = sorted(topMovieList1+topMovieList2, key=lambda rec:rec[-1], reverse=True)
  return topList[:5]

# This is line is important for submitting python jobs through spark-submit!
# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
# Thus the code under the if statement will only be evaluated when the script is called from command line
if __name__ == "__main__":
  sc = SparkContext(appName="Average Rating per Genre")
  ratings = sc.textFile("/share/ml/latest/ratings.csv")
  movieData = sc.textFile("/share/ml/latest/movies.csv")

  movieRatingsCount = ratings.map(extractRating).reduceByKey(sumRatingCount)
  movieGenre = movieData.flatMap(pairMovieToGenre)

  genreRatings = movieGenre.join(movieRatingsCount).values().map(mapToPair)
  
  genreTop5Movies = genreRatings.aggregateByKey([], mergeMaxMovie, mergeCombiners, 1)
  genreTop5Movies.saveAsTextFile("pySparkTop5MoviesPerGenre")
