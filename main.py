'''
	 _____              __
	/  __/__  __ _ ____/ /__
       _\  \/ _ \/ _` / __/ - _/
      /____/  __/\_,_/_/ /_/\_\
      	  /_/

'''      	  
import re, sys, os, pandas, math
from pyspark.sql.functions import when , lit
from matplotlib import pyplot

log_file_path = '/home/mohammed/log/files/'

file_path_list = []
for i in range(1,15):
	if i != 11 :
		file_path_list.append(log_file_path + str(i) +"-1-2017.log")
	#file_path_list.append(log_file_path + str(i) +"-2-2017.log")


base_df = sqlContext.read.json(file_path_list)
'''
[log_file_path+'1-1-2017.log',log_file_path+'2-1-2017.log',log_file_path+'3-1-2017.log',log_file_path+'4-1-2017.log',log_file_path+'5-1-2017.log',log_file_path+'6-1-2017.log',log_file_path+'7-1-2017.log' .....]	
'''
null_column = {}
for column in base_df:
	column_name = str(column)[7:-1]
	null_column[column_name] = base_df.filter(base_df[column_name].isNull()).count()

null_column["context"] = {}

null_column["context"]["user_id"] = base_df.filter(base_df["context.user_id"].isNull()).count()
null_column["context"]["course_id"] = base_df.filter(base_df["context.course_id"].isNull()).count()

null_column["context"]["org_id"] = base_df.filter(base_df["context.org_id"].isNull()).count()
null_column["context"]["path"] = base_df.filter(base_df["context.path"].isNull()).count()

df_beta = base_df.withColumn("willderegister",lit(0))

df_beta.withColumn("will_disenroll",when(df_beta.name=="edx.course.enrollment.deactivated", df_beta.willderegister).otherwise(0))
df_beta = df_beta.drop(df_beta.willderegister)

df_beta = df_beta.withColumn("user_id",df_beta.context.user_id)
df_beta = df_beta.withColumn("course_id",df_beta.context.course_id)
df_beta = df_beta.withColumn("path",df_beta.context.path)

df_beta = df_beta.drop(df_beta.context)
df_beta = df_beta.drop(df_beta.host)
df_beta = df_beta.drop(df_beta.accept_language)


df_temp = df_beta.groupBy("course_id").count()
df_temp = df_temp[df_temp["count"] >= 50000]
unique_courses = [i.course_id for i in df_temp.select('course_id').distinct().collect()]
 
df_gamma = df_beta.where(df_beta.course_id == unique_courses[1]) 

df_gamma = df_gamma.toPandas()
df_gamma["name"].value_counts()
df_gamma["grade"] = -1
df_gamma["age"] = -1

user_data = pandas.read_csv("/home/mohammed/log/log_new/IITBombayX-ME209.1x-1T2017-auth_userprofile-prod-analytics.csv",sep="\t", error_bad_lines=False)
user_data["year_of_birth"].fillna(2018, inplace=True)

for index, row in df_gamma.iterrows():
	if (row["name"]=="edx.course.enrollment.mode_changed") or (row["name"]=="edx.course.enrollment.activated"):
		row = row.copy() 
		start = row["event"].find("user_id")
		stop = row["event"].find("mode")
		df_gamma.loc[index, "user_id"] = float(row["event"][start+9:stop-3])
	if((row["event_type"]=="problem_check") and (row["event_source"]=="server")):
		row = row.copy()
		start = row["event"].find("grade")
		stop = row["event"][start+7:].find(",")
		grade_obtained = float(row["event"][start+7:start+7+stop])
		start = row["event"].find("max_grade")
		stop = row["event"][start+11:].find(",")
		max_grade = float(row["event"][start+11:stop+start+11])
		df_gamma.loc[index, "grade"] = (grade_obtained/max_grade)*100
	try:
		temp1=user_data.loc[user_data["user_id"] == row["user_id"]]["year_of_birth"]
		if(len(temp1)!=0):
			year_of_birth = float(temp1)
		df_gamma.at[index, "age"] = 2017-year_of_birth
	except IndexError:		
		print "does not exist"


df_gamma["age"]

users = df_gamma["username"].unique()

data = {}
user_age = []
performance = []
test_score = []
test_views = []
test_score_deroll = []
test_views_deroll = []
for user in users:
	df_temp = df_gamma.loc[df_gamma["username"] == user]
	df_temp1 = df_temp.loc[(df_gamma["name"] == "play_video") | (df_gamma["name"] == "pause_video") | (df_gamma["name"] == "seek_video") | (df_gamma["name"] == "edx.video.played") | (df_gamma["name"] == "edx.video.paused")]
	TIME = 0
	VIEW_COUNT = 0
	GRADE_COUNT = 0
	VIEW_COUNT_DENROLL = 0
	GRADE_COUNT_DENROLL = 0
	age = 0
	for index, row in df_temp1.iterrows():
		age = row["age"]
		if row["name"] != "seek_video" : 	
			start = row["event"].find("currentTime")
			stop = row["event"][start:].find("}")
			try:
				time = float(row["event"][start+13:-1])
			except ValueError:
				print row["event"]
		if time<TIME and row["name"] != "seek_video" :
			if row["name"] == "pause_video" or  row["name"] == "edx.video.paused":
				VIEW_COUNT-=1
			if row["name"] == "play_video" or row["name"] == "edx.video.played" :
				time = 0
				VIEW_COUNT+=1
		elif row["name"] == "seek_video" :
			start = row["event"].find("new_time")
			stop = row["event"][start:].find(",")
			try:
				TIME = float(row["event"][start+11:start+stop])
			except ValueError :
				print row["event"]
			time = TIME
		else:
			TIME = time
		if row["name"] == "edx.course.enrollment.deactivated":
			VIEW_COUNT_DENROLL = VIEW_COUNT
			GRADE_COUNT_DENROLL = GRADE_COUNT
	for index, row in df_temp.iterrows():
		if row["grade"] != 0 and row["grade"] != -1:
			GRADE_COUNT+=(row["grade"]/100.0)
	if(age!=-1 and VIEW_COUNT <= 202 and GRADE_COUNT <= 271 and VIEW_COUNT>=0 and GRADE_COUNT >= 0):
		user_age.append(age)
		performance.append(math.sqrt(VIEW_COUNT**2 + GRADE_COUNT**2))
	data[user]=[VIEW_COUNT, GRADE_COUNT]
	if(VIEW_COUNT <= 202 and GRADE_COUNT <= 271 and VIEW_COUNT>=0 and GRADE_COUNT >= 0):
		test_views.append(VIEW_COUNT)
		test_score.append(GRADE_COUNT)
		test_views_deroll.append(VIEW_COUNT_DENROLL)
		test_score_deroll.append(GRADE_COUNT_DENROLL)	


pyplot.scatter(test_views ,test_score ,marker='o')
pyplot.scatter(test_views_deroll, test_score_deroll, c='g', marker='o')

pyplot.show()

pyplot.scatter(user_age , performance , marker='o')
pyplot.show()

'''
        name      page session  user_id
182585  None  x_module    None      NaN
182635  None  x_module    None      NaN
212807  None  x_module    None      NaN
212810  None  x_module    None      NaN
212812  None  x_module    None      NaN
212813  None  x_module    None      NaN
302154  None  x_module    None      NaN
302160  None  x_module    None      NaN
'''
df_beta = df_beta.drop(df_beta[((df_beta.user_id.isnull()) & (df_beta.username == "")].index)

pandas.pivot_table(df_beta,values='name',index=["page","referer","ip","name","session","event","course_id","path"],columns=["user_id"])
