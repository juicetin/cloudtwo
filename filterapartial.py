f1 = open('partial.txt', 'r')
f2 = open('partial-f.txt', 'w')
for record in f1:
    photoId, owner, tagList, dateTaken, placeId, accuracy = record.split("\t")
    f2.write(owner + ", " + dateTaken + ", " + placeId + "\n")
