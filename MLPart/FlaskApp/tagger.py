import sys
import csv
import datetime
import math
import pickle
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext


sc = SparkContext("local", "Something")

input_file = open("Final_DVA_311_Requests.csv", 'rU')
data = [list(line) for line in csv.reader(input_file, delimiter=",")]
fieldnames = data[0]

date_index = fieldnames.index('Created Date')
desc_index = fieldnames.index('Descriptor')

#features
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

'''
critical = []

for i in range(1, len(data)):
	critical.append(-1)

for i in range(1, len(data)):
	print i
	cur_date = datetime.datetime.strptime(data[i][date_index], "%m/%d/%y %H:%M")
	for j in range(i+1, len(data)):
		new_date = datetime.datetime.strptime(data[j][date_index], "%m/%d/%y %H:%M")
		if((cur_date - new_date).total_seconds() > 43200):
			break
		if(data[i][comp_index] == data[j][comp_index]):
			if(data[i][desc_index] == data[j][desc_index]):
				if(data[i][incident_index] == data[j][incident_index]):
					if ((data[i][x_index] != "") and (data[j][x_index] != "") and (data[i][y_index] != "") and (data[j][y_index] != "")):
						if(math.sqrt((int(data[i][x_index]) - int(data[j][x_index]))**2 + (int(data[i][y_index]) - int(data[j][y_index]))**2) < 100):
							if(critical[i-1] == -1):
								critical[i-1] = 1
							if(critical[j-1] == -1):
								critical[j-1] = 1

count = 0
for i in range(0,len(critical)):
	if(critical[i] == 1):
		count = count+1
	else:
		critical[i] = 0

print "****************"
print count

f = open("criticalList.txt", "w")
pickle.dump(critical, f)

input_file.close()
f.close()

'''

#Load critical array
f = open("criticalList.txt", "rb")
critical = pickle.load(f)
f.close()

#Write output file
input_file = open("Final_DVA_311_Requests.csv", 'rU')
output_file = open("Tagged_input_final.csv", "w")

i=0
j=0

for line in input_file:
	if i==0:
		content = line.strip().split(',')
		content.append('critical')
		strtemp = ','.join(content)
		output_file.write(strtemp+"\n")
	else:
		line = line.replace(r'\r\n','')
		content = line.strip().split(',')
		content.append(str(critical[j]))
		j=j+1
		strtemp = ','.join(content)
		output_file.write(strtemp+"\n")
	i = i+1

input_file.close()
output_file.close()

#Create a dicts of all categorical features

tagged_file = open("Tagged_input_final.csv", 'rU')
fieldnames_new = tagged_file.next().strip().split(',')
critical_index = fieldnames_new.index('critical')
rows = [list(line) for line in csv.reader(tagged_file, delimiter=",")]
cols = zip(*rows)

dict_agency = {}
i=0
for element in set(cols[agency_index]):
	dict_agency[element.lower()] = i
	i=i+1
with open('dict_agency.pickle', 'wb') as handle:
  pickle.dump(dict_agency, handle)

dict_comp = {}
i=0
for element in set(cols[comp_index]):
	dict_comp[element.lower()] = i
	i=i+1
with open('dict_comp.pickle', 'wb') as handle:
  pickle.dump(dict_comp, handle)


dict_loc = {}
i=0
for element in set(cols[loc_index]):
	dict_loc[element.lower()] = i
	i=i+1
with open('dict_loc.pickle', 'wb') as handle:
  pickle.dump(dict_loc, handle)


dict_incident = {}
i=0
for element in set(cols[incident_index]):
	dict_incident[element] = i
	i=i+1
with open('dict_incident.pickle', 'wb') as handle:
  pickle.dump(dict_incident, handle)

dict_add = {}
i=0
for element in set(cols[add_index]):
	dict_add[element.lower()] = i
	i=i+1
with open('dict_add.pickle', 'wb') as handle:
  pickle.dump(dict_add, handle)

dict_city = {}
i=0
for element in set(cols[city_index]):
	dict_city[element.lower()] = i
	i=i+1
with open('dict_city.pickle', 'wb') as handle:
  pickle.dump(dict_city, handle)

dict_fac = {}
i=0
for element in set(cols[fac_index]):
	dict_fac[element.lower()] = i
	i=i+1
with open('dict_fac.pickle', 'wb') as handle:
  pickle.dump(dict_fac, handle)

dict_status = {}
i=0
for element in set(cols[status_index]):
	dict_status[element.lower()] = i
	i=i+1
with open('dict_status.pickle', 'wb') as handle:
  pickle.dump(dict_status, handle)

dict_bor = {}
i=0
for element in set(cols[bor_index]):
	dict_bor[element.lower()] = i
	i=i+1
with open('dict_bor.pickle', 'wb') as handle:
  pickle.dump(dict_bor, handle)

#print dict_bor

tagged_file.close()


#Prepare final features file
final_output = open("final_output_final.csv", "w")
tagged_file = open("Tagged_input_final.csv", 'rU')
tagged_file.next()

tagged_data = [list(line) for line in csv.reader(tagged_file, delimiter=",")]

for content in tagged_data:
	new_data = []
	#print content
	new_data.append(str(dict_agency[content[agency_index].lower()]))
	new_data.append(str(dict_comp[content[comp_index].lower()]))
	new_data.append(str(dict_loc[content[loc_index].lower()]))
	new_data.append(str(dict_incident[content[incident_index]]))
	new_data.append(str(dict_add[content[add_index].lower()]))
	new_data.append(str(dict_city[content[city_index].lower()]))
	new_data.append(str(dict_fac[content[fac_index].lower()]))
	new_data.append(str(dict_status[content[status_index].lower()]))
	new_data.append(str(dict_bor[content[bor_index].lower()]))
	if(content[x_index] != ""):
		new_data.append(str(content[x_index]))
	else:
		new_data.append(str(0))
	if(content[y_index] != ""):
		new_data.append(str(content[y_index]))
	else:
		new_data.append(str(0))
	new_data.append(str(content[critical_index]))
	new_line = ','.join(new_data)
	final_output.write(new_line+"\n")


tagged_file.close()
final_output.close()

#ML part
def parseLine(line):
    parts = line.split(',')
    label = float(parts[11])
    features = Vectors.dense([float(x) for x in parts[0:11]])
    return LabeledPoint(label, features)

ml_data = sc.textFile('final_output_final.csv').map(parseLine)

(trainingData, testData) = ml_data.randomSplit([0.7, 0.3])

categoricalFeaturesInfo = {0:len(set(cols[agency_index])), 1:len(set(cols[comp_index])), 2:len(set(cols[loc_index])), 3:len(set(cols[incident_index])), 4:len(set(cols[add_index])), 5:len(set(cols[city_index])), 6:len(set(cols[fac_index])), 7:len(set(cols[status_index])), 8:len(set(cols[bor_index]))}
numClasses = 2
numTrees = 3
featureSubsetStrategy="auto"
impurity='gini'
maxDepth=20
maxBins= max(categoricalFeaturesInfo.values()) + 10

model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
                                     numTrees, featureSubsetStrategy,
                                     impurity, maxDepth, maxBins)

model.save(sc, "modelCritical")

predictions = model.predict(testData.map(lambda x: x.features))
#print "HERE I AM"
#print type(predictions.collect())
temp = predictions.collect()
print temp
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v == p).count() / float(testData.count())
print('Accuracy = ' + str(testErr))

'''
predictList = []

f = open("outFile.csv", 'rU')

for line in csv.reader(f, delimiter=","):
	predictRow = list(line)

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

if(agency in dict_agency.keys()):
	predictList.append(dict_agency[agency])


if(complaint in dict_comp.keys()):
	predictList.append(dict_comp[complaint])


if(location in dict_loc.keys()):
	predictList.append(dict_loc[location])


if(incident in dict_incident.keys()):
	predictList.append(dict_incident[incident])

if(address in dict_add.keys()):
	predictList.append(dict_add[address])


if(city in dict_city.keys()):
	predictList.append(dict_city[city])


if(facility in dict_fac.keys()):
	predictList.append(dict_fac[facility])


if(status in dict_status.keys()):
	predictList.append(dict_status[status])


if(borough in dict_bor.keys()):
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

prediction = model.predict(predictData)



print "PREDICTION IS:"
print prediction

f.close()
'''







