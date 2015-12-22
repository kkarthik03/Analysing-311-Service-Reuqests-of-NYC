import sys
import csv
import datetime
import math
import pickle
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext

sc = SparkContext("local", "Something")

'''
input_file = open("Final_DVA_311_Requests.csv", 'rU')
data = [list(line) for line in csv.reader(input_file, delimiter=",")]
fieldnames = data[0]
agency_index = fieldnames.index('Agency')
comp_index = fieldnames.index('Complaint Type')
loc_index = fieldnames.index('Location Type')
incident_index = fieldnames.index('Incident Zip')
add_index = fieldnames.index('Address Type')
city_index = fieldnames.index('City')
fac_index = fieldnames.index('Facility Type')
status_index = fieldnames.index('Status')
bor_index = fieldnames.index('Borough')
x_index = fieldnames.index('X Coordinate (State Plane)')
y_index = fieldnames.index('Y Coordinate (State Plane)')
input_file.close()
'''

with open('dict_agency.pickle', 'rb') as handle:
  dict_agency = pickle.load(handle)

with open('dict_comp.pickle', 'rb') as handle:
  dict_comp = pickle.load(handle)

with open('dict_loc.pickle', 'rb') as handle:
  dict_loc = pickle.load(handle)

with open('dict_incident.pickle', 'rb') as handle:
  dict_incident = pickle.load(handle)

with open('dict_add.pickle', 'rb') as handle:
  dict_add = pickle.load(handle)

with open('dict_city.pickle', 'rb') as handle:
  dict_city = pickle.load(handle)

with open('dict_fac.pickle', 'rb') as handle:
  dict_fac = pickle.load(handle)

with open('dict_status.pickle', 'rb') as handle:
  dict_status = pickle.load(handle)

with open('dict_bor.pickle', 'rb') as handle:
  dict_bor = pickle.load(handle)


model = RandomForestModel.load(sc, "modelCritical")

predictList = []

f = open("outFile3.csv", 'rU')

for line in csv.reader(f, delimiter=","):
	predictRow = list(line)
'''
agency = predictRow[agency_index]
complaint = predictRow[comp_index]
location = predictRow[loc_index]
incident = predictRow[incident_index]
address = predictRow[add_index]
city = predictRow[city_index]
facility = predictRow[fac_index]
status = predictRow[status_index]
borough = predictRow[bor_index]
x_coordinate = predictRow[x_index]
y_coordinate = predictRow[y_index]

'''

agency = predictRow[0]
complaint = predictRow[1]
location = predictRow[2]
incident = predictRow[3]
address = predictRow[4]
city = predictRow[5]
facility = predictRow[6]
status = predictRow[7]
borough = predictRow[8]
x_coordinate = predictRow[9]
y_coordinate = predictRow[10]

if(agency in dict_agency.keys()):
	#print "true agency"
	predictList.append(dict_agency[agency])


if(complaint in dict_comp.keys()):
	#print "true complaint"
	predictList.append(dict_comp[complaint])


if(location in dict_loc.keys()):
	#print "true location"
	predictList.append(dict_loc[location])


if(incident in dict_incident.keys()):
	#print "true incident"
	predictList.append(dict_incident[incident])

if(address in dict_add.keys()):
	#print "true address"
	predictList.append(dict_add[address])


if(city in dict_city.keys()):
	#print "true city"
	predictList.append(dict_city[city])


if(facility in dict_fac.keys()):
	#print "true fac"
	predictList.append(dict_fac[facility])


if(status in dict_status.keys()):
	predictList.append(dict_status[status])
	#print "true status"


if(borough in dict_bor.keys()):
	#print "true borough"
	predictList.append(dict_bor[borough])

if(x_coordinate != ""):
	predictList.append(x_coordinate)
else:
	predictList.append(0)

if(y_coordinate != ""):
	predictList.append(y_coordinate)
else:
	new_data.append(0)


predictData = Vectors.dense([float(x) for x in predictList])
#print predictList
prediction = model.predict(predictData)

#print "PREDICTION IS:"
print prediction

f.close()

