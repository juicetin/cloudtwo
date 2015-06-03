def extractCountry(record):
	try:
		placeId, woeID, latitude, longitude, placeName, placeTypeId, placeURL = record.split("\t")
		placeParts = placeName.split(",")
		country = placeParts[len(placeParts)-1].strip()
		return (placeId, country)
	except:
		return ()

def extractPhoto(record):
	try:
		photoId, owner, tagList, dateTaken, placeId, accuracy = record.split("\t")
		return (photoId, owner, dateTaken)
	except:
		return()


test1 = "beJiSv.bCJ8BLCG7gQ	29389236	42.625	19.133	Danilovgrad, ME, Montenegro	8	/Montenegro/ME/Danilovgrad"
test2 = "l9ihrxSbBZ69pjBn	2425793	36.001	-88.426	Huntingdon, Tennessee, United States	7	/United+States/Tennessee/Huntingdon"
test3 = "4943843837	35386145@N05	france phonebooth telephone rainstorm	2009-06-08 03:03:03	jxy9exSfA57Aj0w	16"
print (extractCountry(test2))
print (extractPhoto(test3))