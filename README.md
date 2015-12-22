# Analysing-311-Service-Reuqests-of-NYC

This folder contains the two zip files for machine learning part and the visualization part correspondingly

The dataset used can be found in the below link
https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
The total size of the data set we used was about 6GB. After data cleaning we brought it down to about 800 MB. This cleaned data was used in the ML classification algo. So we cannot attach the dataset with the project due to it's size.

The visualization uses the data on the website using an API so it can run as long as internet is available.

The vizualization demo can be run by extracting the 311project.zip and doing the following
1) If you have firefox it can be run without a webserver(requires internet though).
2) If you have only chrome then it can be run using the simple python httpserver that was used in HW2. Navigate to the html folder on the server and the demo should work
3) We have a video of the demo at https://youtu.be/LL3jhbixUcc


The machine learning zip contains a FlaskApp folder which can be run like below : 

To run this 
Install Spark. Just download spark from the apache spark site. export SPARK_HOME=*path* and add export PATH=$SPARK_HOME/bin:$PATH
pip install flask
Go inside FlaskApp and just run python app.py
The form will be available at localhost:5000/
YOU DONíT NEED TO RUN tagger.py as the model is already stored and ready

The main ML part consists of the following files (The mentioned csv files are too large to attach so the below code can't be executed)

tagger.py does the following :
 reads the input file Final_DVA_311_Requests.csv and first creates Tagged_input_final.csv (with an extra column called ìCriticalî)
It then converts the fields - agency, complaint type, location type etc into categorical features by creating dictionaries (mapping the different possible values to numbers). These dicts are stored as pickle files. The x and y coordinates are treated as continuous features
The final feature set is written to final_output_final.csv 
The Random Forest classifier is trained using this feature set 70% is held out for train and 30% for test and the accuracy is calculated from test set prediction
The final model is stored as modelCritical

app.py does the following:
Loads index.html
When the signUp button is clicked, retrieves form features and writes to outFile3.txt and calls predicter.py as an argument to spark-submit. 
The prediction returned is either 0.0 or 1.0
This is rendered into a dialog box using signUpNew.js present under the static->js
predicter.py does the following:
Takes the features entered into the form from outFile3.csv
Loads the dictionaries for categorical features and converts categorical features to their representations
Loads the model and passes the input feature set for prediction
Produces the prediction output


