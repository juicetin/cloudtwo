#	Find how many times a user has visited a country, as well as the max/min/avg/total time spent in that country
#	A user is considered having visited a place if they have taken at least one photo in that place
#	A visit in a country lasts from when the first photo is taken until the first one is taken in a different country

from pyspark import SparkContext

def extractCountry(record):
    try:
        placeId, woeID, latitude, longitude, placeName, placeTypeId, placeURL = record.split("\t")
        placeParts = placeName.split(",")
        country = placeParts[len(placeParts)-1]
        return (placeId, country)
    except:
        return ()

def extractUserPhotos(record):
    try:
        photoId, owner, tagList, dateTaken, placeId, accuracy = record.split("\t")
        return (placeId, (owner, dateTaken))
    except:
        return()

def dateAsKey(record):
    (owner, dateTaken), country = record
    return (dateTaken, (owner, country)) 

def mapListTmp(record):
    user, dateCountries = record
    value_list = []
    for pair in dateCountries:
        value_list.append(pair)
    return(user, value_list)

def setUserAsKey(record):
    date, (user, country) = record
    return (user, (date, country))


if __name__ == "__main__":
    sc = SparkContext(appName="Country visits per user")
    places = sc.textFile("/share/place.txt")
    photos = sc.textFile("/share/small/partial.txt")

    # Even immediate save brings file from 73 to 37 lines
    # Gets exactly the first 37 lines - do the following ones not use the correct type of newline or something?
    photos.saveAsTextFile("a-photos")

    # Contains (placeId, country) pairs
    # (u'3CHWt66YA5rI2cdWdA', u' France')
    # (u'nQym.fSbCZlvr_lBwg', u' Japan')
    # (u'6dAqkmqbBZ7qI0ZM', u' United States')
    # (u'x3_kyz.cBJQqKKiEsg', u' United States')
    place_lookup = places.map(extractCountry)
    place_lookup.saveAsTextFile("a-placelookup")

    # Contains (placeId, (owner, dateTaken)) pairs
    # (u'hyYdCAGYApraBjg', (u'8036172@N05', u'2006-04-11 19:02:22'))
    # (u'Cfn4WbmYAJwmFepC', (u'21577727@N05', u'2010-01-26 21:06:35'))
    # (u'uZNgdHucCJ5htIw', (u'7755447@N05', u'2007-06-07 16:54:21'))
    # (u'Cfn4WbmYAJwmFepC', (u'21577727@N05', u'2010-01-26 21:06:28'))
    # (u'JZJzq.KbBZpBs0Rb', (u'35386145@N05', u'2010-11-05 06:48:40'))
    prelim_photos = photos.map(extractUserPhotos)
    prelim_photos.saveAsTextFile("a-prelimphotos")

    # Replace place IDs with place name to create (date, (owner, country))
    #(u'2006-01-27 23:48:59', (u'8036172@N05', u' Mexico'))
    #(u'2006-03-11 21:44:48', (u'8036172@N05', u' Mexico'))
    #(u'2006-04-11 19:02:22', (u'8036172@N05', u' Mexico'))
    #(u'2006-04-29 14:35:13', (u'8036172@N05', u' Mexico'))
    #(u'2007-01-26 11:17:16', (u'8036172@N05', u' Mexico'))
    #(u'2007-02-11 16:09:53', (u'7755447@N05', u' France'))
    #(u'2007-02-21 02:16:36', (u'7556490@N05', u' United Kingdom'))
    #(u'2007-02-21 02:20:03', (u'7556490@N05', u' United Kingdom'))
    #(u'2007-04-26 15:00:00', (u'35386145@N05', u' United States'))
    #(u'2007-05-17 15:10:43', (u'7755447@N05', u' France'))
    #(u'2007-05-21 13:24:05', (u'7755447@N05', u' France'))
    #(u'2007-06-03 13:11:56', (u'7755447@N05', u' France'))
    #(u'2007-06-03 18:05:30', (u'7755447@N05', u' France'))
    #(u'2007-06-03 18:44:23', (u'7755447@N05', u' France'))
    #(u'2007-06-06 14:21:54', (u'7755447@N05', u' France'))
    #(u'2007-06-07 16:54:21', (u'7755447@N05', u' France'))
    #(u'2007-06-09 10:11:32', (u'7755447@N05', u' France'))
    #(u'2007-06-09 13:51:32', (u'7755447@N05', u' France'))
    #(u'2007-07-03 12:45:36', (u'7755447@N05', u' France'))
    country_photos_byDate = prelim_photos.join(place_lookup).values().map(dateAsKey).sortByKey()
    country_photos_byDate.saveAsTextFile("a-photosByDate")

    #  Swap order back to (owner, (date, country)) now that date is ordered
    #(u'8036172@N05', (u'2006-01-27 23:48:59', u' Mexico'))
    #(u'8036172@N05', (u'2006-03-11 21:44:48', u' Mexico'))
    #(u'8036172@N05', (u'2006-04-11 19:02:22', u' Mexico'))
    #(u'8036172@N05', (u'2006-04-29 14:35:13', u' Mexico'))
    #(u'8036172@N05', (u'2007-01-26 11:17:16', u' Mexico'))
    #(u'7755447@N05', (u'2007-02-11 16:09:53', u' France'))
    #(u'7556490@N05', (u'2007-02-21 02:16:36', u' United Kingdom'))
    #(u'7556490@N05', (u'2007-02-21 02:20:03', u' United Kingdom'))
    #(u'35386145@N05', (u'2007-04-26 15:00:00', u' United States'))
    #(u'7755447@N05', (u'2007-05-17 15:10:43', u' France'))
    #(u'7755447@N05', (u'2007-05-21 13:24:05', u' France'))
    #(u'7755447@N05', (u'2007-06-03 13:11:56', u' France'))
    #(u'7755447@N05', (u'2007-06-03 18:05:30', u' France'))
    #(u'7755447@N05', (u'2007-06-03 18:44:23', u' France'))
    #(u'7755447@N05', (u'2007-06-06 14:21:54', u' France'))
    #(u'7755447@N05', (u'2007-06-07 16:54:21', u' France'))
    #(u'7755447@N05', (u'2007-06-09 10:11:32', u' France'))
    #(u'7755447@N05', (u'2007-06-09 13:51:32', u' France'))
    #(u'7755447@N05', (u'2007-07-03 12:45:36', u' France'))
    country_photos_byUser = country_photos_byDate.map(setUserAsKey).sortByKey()
    country_photos_byUser.saveAsTextFile("a-photosByUser")

    # Group each date/country pair as an iterable list per user
    #(u'7556490@N05', [(u'2007-02-21 02:16:36', u' United Kingdom'), (u'2007-02-21 02:20:03', u' United Kingdom')])
    #(u'35386145@N05', [(u'2009-04-01 04:42:00', u' United States'), 
                      # (u'2009-05-02 05:15:45', u' United States'), 
                      # (u'2009-06-08 03:03:03', u' France'), 
                      # (u'2009-12-23 04:57:32', u' United States'), 
                      # (u'2010-07-26 06:11:12', u' United States'), 
                      # (u'2010-07-26 10:30:09', u' United States'), 
                      # (u'2010-08-26 02:22:59', u' United States'), 
                      # (u'2010-10-08 08:46:58', u' United States'), 
                      # (u'2010-10-31 11:11:29', u' United States'), 
                      # (u'2010-11-05 06:48:40', u' United States'), 
                      # (u'2010-12-28 08:38:03', u' United States'), 
                      # (u'2008-10-19 15:13:09', u' United States'), 
                      # (u'2009-03-18 03:34:56', u' United States'), 
                      # (u'2007-04-26 15:00:00', u' United States')])
    # or
    # (u'35386145@N05', [(u'2007-04-26 15:00:00', u' United States'), 
                       # (u'2008-10-19 15:13:09', u' United States'), 
                       # (u'2009-03-18 03:34:56', u' United States'), 
                       # (u'2009-04-01 04:42:00', u' United States'), 
                       # (u'2009-05-02 05:15:45', u' United States'), 
                       # (u'2009-06-08 03:03:03', u' France'), 
                       # (u'2009-12-23 04:57:32', u' United States'), 
                       # (u'2010-07-26 06:11:12', u' United States'), 
                       # (u'2010-07-26 10:30:09', u' United States'), 
                       # (u'2010-08-26 02:22:59', u' United States'), 
                       # (u'2010-10-08 08:46:58', u' United States'), 
                       # (u'2010-10-31 11:11:29', u' United States'), 
                       # (u'2010-11-05 06:48:40', u' United States'), 
                       # (u'2010-12-28 08:38:03', u' United States')])
    # (u'7556490@N05', [(u'2007-02-21 02:16:36', u' United Kingdom'), 
                      # (u'2007-02-21 02:20:03', u' United Kingdom')])


    #NOTE needs groupByKey().map(mapListTmp) to show like the above, otherwise random addresses
    #NOTE doesn't seem to be in order like it should be...
    user_list = country_photos_byUser.groupByKey().map(mapListTmp)
    user_list.saveAsTextFile("a-userlist")

