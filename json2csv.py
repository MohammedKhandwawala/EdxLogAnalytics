import csv
import json

for i in range(1,8):
	name=str(i)+"-1-2017"
	infile = open(name+".log", "r")
	outfile = open(name+".csv", "w")

	f = csv.writer(outfile)	

	logs=[]

	for line in infile:
		logs.append(json.loads(line))

	f.writerow(["username","event_source","name","accept_language","time","agent","page","host","referer","context_user_id","context_org_id","context_path","ip","event","event_type"])

	for log in logs:
		if log.has_key("name")!=True:
			log["name"]=None
		f.writerow([log["username"],log["event_source"],log["name"],log["accept_language"],log["time"],log["agent"],log["page"],log["host"],log["referer"],log["context"]["user_id"],log["context"]["org_id"],log["context"]["course_id"],log["context"]["path"],log["ip"],log["event"],log["event_type"]])
	#print log["event_type"].replace(";","_")
#print logs[81]
