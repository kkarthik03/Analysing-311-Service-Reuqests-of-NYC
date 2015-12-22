from flask import Flask, render_template, request, json
import csv
import os
from subprocess import check_output


app = Flask(__name__)

@app.route("/")
def main():
	return render_template('index.html')

def parseLine(line):
	parts = line.split(',')
	label = float(parts[11])
	features = Vectors.dense([float(x) for x in parts[0:11]])
	return LabeledPoint(label, features)

@app.route('/signUpNew',methods=['POST'])
def signUp():
	f = open("outFile3.csv", "w")
	_agency = request.form['inputAgency']
	_complaint = request.form['inputComplaintType']
	_location = request.form['inputLocationType']
	_incident = request.form['inputIncidentZip']
	_address = request.form['inputAddrType']
	_city = request.form['inputCity']
	_facility = request.form['inputFacilityType']
	_status = request.form['inputStatus']
	_borough = request.form['inputBorough']
	_xcoord = request.form['inputXCoord']
	_ycoord = request.form['inputYCoord']
	if _agency and _complaint and _location and _incident and _address and _city and _facility and _status and _borough:
		writer = csv.writer(f, delimiter = ',')
		writer.writerow([_agency.lower(),_complaint.lower(),_location.lower(),_incident,_address.lower(),_city.lower(),_facility.lower(),_status.lower(),_borough.lower(),_xcoord,_ycoord])
		f.close()
		out = check_output(["spark-submit", "predicter.py"])
		out = out.strip()
		if(out == "0.0"):
			outString = "Prediction for this service request is not critical"
		else:
			outString = "Prediction for this service request is critical"
		#return json.dumps({'Prediction for this service request is': out})
		return outString
	else:
		return json.dumps({'html':'<span>Enter the required fields</span>'})



if __name__ == "__main__":
	app.run()
