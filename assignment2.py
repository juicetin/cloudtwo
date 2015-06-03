#	Find how many times a user has visited a country, as well as the max/min/avg/total time spent in that country
#	A user is considered having visited a place if they have taken at least one photo in that place
#	A visit in a country lasts from when the first photo is taken until the first one is taken in a different country

import sys
from pyspark import SparkContext
import datetime

class IteratorEx(object):
    def __init__(self, it):
        self.it = iter(it)
        self.sentinel = object()
        self.nextItem = next(self.it, self.sentinel)
        self.hasNext = self.nextItem is not self.sentinel

    def next(self):
        ret, self.nextItem = self.nextItem, next(self.it, self.sentinel)
        self.hasNext = self.nextItem is not self.sentinel
        return ret

    def __iter__(self):
        while self.hasNext:
            yield self.next()

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

# Swap key/value tuples around, from ((a, b), c) to ((
def userDateKey(record):
    (owner, dateTaken), country = record
    return ((owner, dateTaken), country)

def mapListTmp(record):
    user, dateCountries = record
    value_list = []
    for pair in dateCountries:
        value_list.append(pair)
    return(user, sorted(value_list))

def validDate(date):
    date_parts = date.split("-")
    return int(date_parts[0]) > 1970 and int(date_parts[1]) > 0

def summariseVisitList(record):
    user, pairs = record
    summarised = []

    # Store cur country and date to keep track of changing location
    cur_country = [""]
    cur_first_date = [None]
    for pair in pairs:
        cur_country[0] = pair[0]
        try:
            cur_first_date[0] = datetime.datetime.strptime(pair[1], "%Y-%m-%d %H:%M:%S")
            break
        except ValueError:
            continue
            
        # Only get first item in iterable - requires loop as iterable cannot be accessed by index 

    # Iterate through all photos
    iterex = IteratorEx(pairs)
    for pair in iterex:
        country, date = pair 
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

        # Country change - update visits, max, min, total
        # Missing an "or" condition - country != cur_country or next-value-exists() (need to write my own iterator container)
        # http://stackoverflow.com/questions/15226967/last-element-in-a-python-iterator
        if country != cur_country[0] or not iterex.hasNext:
            for sublist in summarised:
                # Modify country entry as previous one has ended
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

        # When the last photo changes country (and hence has visit time 0)
        if country != tmp_cur_country[0] and not iterex.hasNext:
            for sublist in summarised:
                if sublist[0] == country:
                    sublist[1] += 1
                    sublist[3] = 0

    # Get average once all visits have been tallied and totalled
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


def userAsKey(record):
    (user, date), country = record
    return (user, (country, date))

if __name__ == "__main__":
    sc = SparkContext(appName="Country visits per user")
    places = sc.textFile("/share/place.txt")
    photos = sc.textFile(sys.argv[1])

    # Extracts (placeId, country) tuples from places file
    place_lookup = places.map(extractCountry)

    # Extracts (placeId, owner, dateTaken)) tuples from photos file
    prelim_photos = photos.map(extractUserPhotos)

    # Join extracted photos with places and get values to replace placeIds with countries
    # Map resulting value containing countries, using (user, date) tuples as the key and sorting as such
    # This gives us a sorted ((user, date), country) result
    country_photos_byDate = prelim_photos.join(place_lookup).values().map(userDateKey).sortByKey()

    # Swap order to get (owner, (country, date)) tuples
    country_photos_byUser = country_photos_byDate.map(userAsKey)

    # Group by key to get (owner, (country, date), (country, date)...) tuples
    # Operate on (country, date),... iterable to get visit stats
    user_visit_list = country_photos_byUser.groupByKey().map(summariseVisitList)
    user_visit_list.saveAsTextFile("a-uservisitlist")

