import json

with open("goog.json") as jsonfile:
    js = json.load(jsonfile)      

# write a new file with one object per line
with open("goog-flattened.json", 'a') as outfile:
    for d in js:
        json.dump(d, outfile)
        outfile.write('\n')
