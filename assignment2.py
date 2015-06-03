#	Find how many times a user has visited a country, as well as the max/min/avg/total time spent in that country
#	A user is considered having visited a place if they have taken at least one photo in that place
#	A visit in a country lasts from when the first photo is taken until the first one is taken in a different country

import sys
from pyspark import SparkContext
import datetime

# Takes a flickr places info file and extracts (placeId, country)
def extractCountry(record):
    try:
        placeId, woeID, latitude, longitude, placeName, placeTypeId, placeURL = record.split("\t")
        placeParts = placeName.split(",")
        country = placeParts[len(placeParts)-1]
        return (placeId, country)
    except:
        return ()

# Takes flickr photo data and extracts (placeId, (owner, dateTaken))
def extractUserPhotos(record):
    try:
        photoId, owner, tagList, dateTaken, placeId, accuracy = record.split("\t")
        return (placeId, (owner, dateTaken))
    except:
        return()

def sortPhotoList(record):
    user, pairs = record
    photos = []
    for photo in pairs:
        photos.append(photo)
    return (user, sorted(photos))

def summariseVisitList(record):
    user, pairs = record
    summarised = []

    # Store cur country and date to keep track of changing location
    cur_country = [""]
    cur_first_date = [None]
    for pair in pairs:
        cur_country[0] = pair[1]

        # Get the first vaild date in the list of photos per user
        try:
            cur_first_date[0] = datetime.datetime.strptime(pair[0], "%Y-%m-%d %H:%M:%S")
            break
        except ValueError:
            continue
            
    # Iterate through all photos
    for i, pair in enumerate(pairs):
        date, country = pair
        tmp_cur_country = [""]
        try:
            tmp = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        # Flag to check if country is in list yet
        found = False
        for sublist in summarised:
            if sublist[0] == country:
                found = True
                break

        # Add country visit stats to list if it doesn't yet exist
        if found == False:
            summarised.append([country, 0, 0, sys.maxsize, 0, 0])

        # Whenever country changes, or we are at the last element for a user
        if country != cur_country[0] or i == len(pairs) - 1:
            for sublist in summarised:
                
                #   Modify a country's stats for the user once a new country is visited
                try:
                    if sublist[0] == cur_country[0]:
                        sublist[1] += 1

                        new_first_time = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                        time_spent = new_first_time - cur_first_date[0]

                        seconds = time_spent.total_seconds()

                        # Modify max visit time for country
                        if seconds > sublist[2]:
                            sublist[2] = seconds 

                        # Modify min visit time for country
                        if seconds < sublist[3]:
                            sublist[3] = seconds 

                        # Update total time for country
                        sublist[5] += seconds 

                        # Update new current country and start time
                        tmp_cur_country[0] = cur_country[0]
                        cur_country[0] = country
                        cur_first_date[0] = new_first_time
                        break

                except ValueError:
                    break

        # When the last photo changes country (and hence has visit time 0, also the new min)
        if country != tmp_cur_country[0] and i == len(pairs) - 1:
            for sublist in summarised:
                if sublist[0] == country:
                    sublist[1] += 1
                    sublist[3] = 0

    # Get average once all visits have been tallied and totalled, convert to days
    for sublist in summarised:
        if (sublist[1] > 0):
            sublist[4] = sublist[5] / sublist[1]
        sublist[2] = round(sublist[2] / 86400.0, 1)
        if sublist[3] == sys.maxsize:
            sublist[3] = 0 
        sublist[3] = round(sublist[3] / 86400.0, 1)
        sublist[4] = round(sublist[4] / 86400.0, 1)
        sublist[5] = round(sublist[5] / 86400.0, 1)

    return (user, summarised)

#   Convert records with user,date keys to user keys
def userAsKey(record):
    (user, date), country = record
    return (user, (date, country))

if __name__ == "__main__":

    sc = SparkContext(appName="Country visits per user")
    places = sc.textFile("/share/place.txt")
    files = []
    for photo_file in sys.argv:
        files.append(photo_file)
    files = files[1:]
    #photos = sc.textFile(','.join(files[1:]))
    photos = sc.union([sc.textFile(f) for f in files])

    # Extracts (placeId, country) tuples from places file
    place_lookup = places.map(extractCountry)

    # Extracts (placeId, owner, dateTaken)) tuples from photos file
    prelim_photos = photos.map(extractUserPhotos)

    # Join extracted photos with places and get values to replace placeIds with countries
    # Map resulting value containing countries, using (user, date) tuples as the key and sorting as such
    # This gives us a sorted ((user, date), country) result
    country_photos_byDate = prelim_photos.join(place_lookup).values()

    # Swap order to get (owner, (date, country)) tuples
    country_photos_byUser = country_photos_byDate.map(userAsKey)

    # Group by key to get (owner, (country, date), (country, date)...) tuples
    # Operate on (country, date),... iterable to get visit stats
    sorted_visit_list = country_photos_byUser.groupByKey().map(sortPhotoList)

    user_visit_list = sorted_visit_list.map(summariseVisitList)
    user_visit_list.saveAsTextFile("a-uservisitlist")

