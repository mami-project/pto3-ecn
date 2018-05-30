import glob
import json

for filename in glob.glob("*.pto_file_metadata.json"):
    try:
        nameparts = filename.split(".")[0].split("-")
        if nameparts[1] == 'do':
            vantage = "digitalocean-"+nameparts[2]
        else:
            continue
    except IndexError:
        continue
    
    with open(filename) as mdfile:
        mdj = json.load(mdfile)
        mdj['vantage'] = vantage

    with open(filename + ".new", mode="w") as newmdfile:
        json.dump(mdj, newmdfile)

    # stdout gets commands to change the metadata after we're happy with it
    print("mv {0}.new {0}\n".format(filename))