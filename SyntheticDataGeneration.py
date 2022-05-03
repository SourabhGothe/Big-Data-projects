import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from functools import reduce  
from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
from pyspark.sql.functions import regexp_replace

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def buildHygieneScoreGetTop10(states, data, axes1=None, index=0):
  answer = {}
  
  for state in states:
    stateData = data[state]
    stateData = stateData.select("State_Name", "District_Name", "Total_Number_of_Schools", "Functional_Toilet_Facility", "Functional_Urinal", "Functional_Drinking_Water", "Water_Purifier", "Water_Tested", "Handwash")
    stateData = stateData.withColumn("HygieneScoreAvg", F.expr("(100 * ((Functional_Drinking_Water * 0.25) +\
                                                                  (Water_Purifier * 0.25) +\
                                                                  (Functional_Toilet_Facility * 0.25) +\
                                                                  (Handwash * 0.15) +\
                                                                  (Functional_Urinal * 0.05) +\
                                                                  (Water_Tested * 0.05)) / Total_Number_of_Schools)"))
    
    stateData = stateData.groupBy("State_Name", "District_Name").avg("HygieneScoreAvg").withColumnRenamed("avg(HygieneScoreAvg)", "HygieneScore")
    
    answer[state] = stateData
  current_year = data[states[0]].first()['Academic_Year']

  allStatesData = [answer[state] for state in states]
  allStatesData = unionAll(*allStatesData)
  qa = allStatesData.orderBy(col("HygieneScore").desc())
  qa_answer = []
  qb_answer = []
  
  for i in qa.take(10):
    qa_answer.append(tuple(i))
  
  qbAvg = allStatesData.groupBy().avg("HygieneScore").withColumnRenamed("avg(HygieneScore)", "HygieneScoreAverage")
  qbAvg = qbAvg.first()['HygieneScoreAverage']
  qb = allStatesData.select("District_Name", "HygieneScore").where(col("HygieneScore") > qbAvg).orderBy(col("HygieneScore").desc())
  for i in qb.take(10):
    qb_answer.append(tuple(i))

  allStatesDistribution = allStatesData.select("State_Name", "HygieneScore").withColumnRenamed("State_Name", str(current_year))

  #Do not plot for synthetic data
  if "A" not in states:
    allStatesDistribution = allStatesDistribution.toPandas()
    allStatesDistribution.HygieneScore=pd.to_numeric(allStatesDistribution.HygieneScore)
    allStatesDistribution.boxplot(by=current_year, column =['HygieneScore'], ax=axes1[index])
  
  allStatesDataInfo = allStatesData.groupBy("State_Name").avg("HygieneScore").withColumnRenamed("avg(HygieneScore)", "HygieneScoreVal")\
  .select("State_Name", "HygieneScoreVal").orderBy(col("State_Name"))

  synthValidation = []
  for i in allStatesDataInfo.take(10):
    synthValidation.append(tuple(i))
  return qa_answer, qb_answer, answer, allStatesDataInfo, synthValidation


class SyntheticDataGeneration:
	def generateSyntheticDataQ1():
	  requiredFields = ["State_Name", "District_Name", "Location", "School_Management_Name", "School_Management_Id", "Total_Number_of_Schools", "Academic_Year"]

	  realData = readAllDataForState("Karnataka") #This is just to get the column names/schema/counts, could be any state
	  schema = realData.schema
	  pds = realData.toPandas()
	  syntheticData = spark.createDataFrame(pds, schema=schema)

	  managementIds = syntheticData.select("School_Management_Id", "School_Management_Name").distinct().orderBy("School_Management_Id")
	  mgtData=[]
	  for i in managementIds.collect():
		mgtData.append(tuple(i))

	  Academic_Year = 2017
	  vals = []
	  ch = 'a'
	  lenOfmanagement = len(mgtData)
	  totalNumberOfRowsPerState = 10 * 10 #100
	  syntheticStates = ["A", "B", "C"]
	  config = {'A':{'Urban': 0.6, 'Rural': 0.4}, 'B':{'Urban': 0.3, 'Rural': 0.7}, 'C':{'Urban': 0.2, 'Rural': 0.8}}
	  data = {}
	  for State in syntheticStates:
		vals = []
		Location = "Rural"
		for i in range(0, int(totalNumberOfRowsPerState*config[State]['Rural'])):#40
		  District_Name = chr(ord(ch) + i%26)
		  School_Management_Id = mgtData[i % lenOfmanagement][0]
		  School_Management_Name = mgtData[i % lenOfmanagement][1]
		  val = (State, District_Name, Location, School_Management_Name, School_Management_Id, 1, Academic_Year)
		  vals.append(val)

		Location = "Urban"
		for i in range(0, int(totalNumberOfRowsPerState*config[State]['Urban'])):#60
			District_Name = chr(ord(ch) + i%26)
			School_Management_Id = mgtData[i % lenOfmanagement][0]
			School_Management_Name = mgtData[i % lenOfmanagement][1]
			val = (State, District_Name, Location, School_Management_Name, School_Management_Id, 1, Academic_Year)
			vals.append(val)
		Academic_Year +=1
		
		data[State] = spark.createDataFrame(vals, requiredFields)
	  return data, config

	def generateSyntheticDataQ2(state, year="2019_2020"):
	  requiredFields = ['State_Name', 'Total_Number_of_Schools', 'Functional_Electricity', 'Playground', 'Library_or_Reading_Corner_or_Book_Bank', 'Newspaper', 'Functional_Boy_Toilet', 'Functional_Girl_Toilet', 'Functional_Toilet_Facility', 'Functional_Urinal_Boy', 'Functional_Urinal_Girl', 'Functional_Urinal', 'Functional_Toilet_and_Urinal', 'Functional_Drinking_Water', 'Water_Purifier', 'Handwash', 'Ramps', 'Internet', 'Computer_Available']
	  realData = readAllDataForState(state)
	  schema = realData.schema
	  #print(schema.names)
	  pds = realData.toPandas()
	  syntheticData = spark.createDataFrame(pds, schema=schema)
	  syntheticStates = ["A", "B", "C"]
	  syntheticData = syntheticData.select(requiredFields)
	  syntheticData = syntheticData.filter(syntheticData.State_Name == "")
	  #syntheticData.show()
	  vals = []
	  totalNumberOfSchoolsPerState = 10000#00
	  config = {'A':{'Functional_Electricity': 0.7, 'Playground':0.8, 'Library_or_Reading_Corner_or_Book_Bank':0.45, 'Newspaper':0.30, 'Functional_Boy_Toilet':0.70, 'Functional_Girl_Toilet':0.80, 'Functional_Toilet_Facility':0.85, 'Functional_Urinal_Boy':0.85, 'Functional_Urinal_Girl':0.70, 'Functional_Urinal':0.80, 'Functional_Toilet_and_Urinal':0.85, 'Functional_Drinking_Water':0.40, 'Water_Purifier':0.25, 'Handwash':0.60, 'Ramps':0.11, 'Internet':0.15, 'Computer_Available':0.20},
				'B':{'Functional_Electricity': 0.6, 'Playground':0.7, 'Library_or_Reading_Corner_or_Book_Bank':0.35, 'Newspaper':0.20, 'Functional_Boy_Toilet':0.60, 'Functional_Girl_Toilet':0.70, 'Functional_Toilet_Facility':0.75, 'Functional_Urinal_Boy':0.75, 'Functional_Urinal_Girl':0.60, 'Functional_Urinal':0.70, 'Functional_Toilet_and_Urinal':0.75, 'Functional_Drinking_Water':0.30, 'Water_Purifier':0.15, 'Handwash':0.50, 'Ramps':0.09, 'Internet':0.10, 'Computer_Available':0.15}, 
				'C':{'Functional_Electricity': 0.5, 'Playground':0.6, 'Library_or_Reading_Corner_or_Book_Bank':0.25, 'Newspaper':0.10, 'Functional_Boy_Toilet':0.50, 'Functional_Girl_Toilet':0.60, 'Functional_Toilet_Facility':0.65, 'Functional_Urinal_Boy':0.65, 'Functional_Urinal_Girl':0.50, 'Functional_Urinal':0.60, 'Functional_Toilet_and_Urinal':0.65, 'Functional_Drinking_Water':0.20, 'Water_Purifier':0.05, 'Handwash':0.40, 'Ramps':0.65, 'Internet':0.05, 'Computer_Available':0.10}}

	  data = []
	  for State in syntheticStates:
		facilitiesAndPercentages = config[State]
		facilitiesAndData = {}
		for key in facilitiesAndPercentages.keys():
		  arr = np.zeros(totalNumberOfSchoolsPerState)
		  endIndex = int(facilitiesAndPercentages[key] * totalNumberOfSchoolsPerState)
		  arr[:endIndex] = 1
		  np.random.shuffle(arr)
		  facilitiesAndData[key] = arr.tolist()

		for i in range(0, totalNumberOfSchoolsPerState):
		  line = []
		  line.append(State)#State_Name
		  line.append(1)#Total_Number_of_Schools
		  for key in requiredFields:
			if key == 'State_Name' or key == 'Total_Number_of_Schools':
			  continue
			line.append(facilitiesAndData[key][i])
		  data.append(tuple(line))

	  newRow = spark.createDataFrame(data, requiredFields)
	  syntheticData = syntheticData.union(newRow)
	  #syntheticData.show(1)
	  #print(syntheticData.count())

	  return syntheticData, config

	#Q3 Verification
	def generateSyntheticDataQ3(totalNumberOfRowsPerState=100):
	  """
	  Weightage of features:
	  Functional_Toilet_Facility : [0.25], Functional_Urinal : [0.05], Functional_Drinking_Water : [0.25]
	  Water_Purifier : [0.25], Water_Tested : [0.05], Handwash : [0.15]
	  """

	  requiredFields = ["State_Name", "District_Name", "Location", "Total_Number_of_Schools", "Functional_Toilet_Facility", "Functional_Urinal",\
						"Functional_Drinking_Water", "Water_Purifier", "Water_Tested", "Handwash", "Academic_Year"]

	  realData = readAllDataForState("Karnataka") #This is just to get the column names/schema, could be any state
	  schema = realData.schema
	  pds = realData.toPandas()
	  syntheticData = spark.createDataFrame(pds, schema=schema)

	  vals = []
	  ch = 'a'

	  Academic_Year = 2017
	  syntheticStates = ["A", "B", "C"]
	  config = {'A':[1, 1, 1, 1, 1, 1, 0], 'B':[1, 1, 1, 1, 1, 0, 0], 'C':[1, 1, 1, 1, 0, 1, 0]}
	  configWeightage = [0, 0.25, 0.05, 0.25, 0.25, 0.05, 0.15]

	  LocationList = ["Rural", "Urban"]
	  data = {}
	  for State in syntheticStates:
		vals = []
		for i in range(0, int(totalNumberOfRowsPerState)):
		  District_Name = chr(ord(ch) + i%26)
		  Location = LocationList[i % 2]
		  configValues = config[State]
		  Total_Number_of_Schools = configValues[0]
		  Functional_Toilet_Facility = configValues[1]
		  Functional_Urinal = configValues[2]
		  Functional_Drinking_Water = configValues[3]
		  Water_Purifier = configValues[4]
		  Water_Tested = configValues[5]
		  Handwash = configValues[6]
		  val = (State, District_Name, Location, Total_Number_of_Schools, Functional_Toilet_Facility, Functional_Urinal, Functional_Drinking_Water,\
				 Water_Purifier, Water_Tested, Handwash,Academic_Year)
		  vals.append(val)
		  Academic_Year +=1
		
		data[State] = spark.createDataFrame(vals, requiredFields)
	  return data, config, configWeightage

	#Q4 Verification
	def generateSyntheticDataQ4(totalNumberOfRowsPerState=1000):

	  """
	  Weightage of featurs:
	  Solar_Panel : [0.2], Kitchen_Garden : [0.3], Rain_Water_Harvesting : [0.4]
	  Incinerator : [0.1]
	  """
	  requiredFields = ["State_Name", "District_Name", "Total_Number_of_Schools", "Solar_Panel", "Kitchen_Garden", "Rain_Water_Harvesting", "Incinerator", "Academic_Year"]

	  realData = readAllDataForState("Karnataka") #This is just to get the column names/schema, could be any state
	  schema = realData.schema
	  pds = realData.toPandas()
	  syntheticData = spark.createDataFrame(pds, schema=schema)

	  managementIds = syntheticData.select("School_Management_Id", "School_Management_Name").distinct().orderBy("School_Management_Id")
	  mgtData=[]
	  for i in managementIds.collect():
		mgtData.append(tuple(i))

	  vals = []
	  ch = 'a'
	  Academic_Year = 2017
	  lenOfmanagement = len(mgtData)
	  syntheticStates = ["A", "B", "C"]
	  "State_Name", "District_Name", "Total_Number_of_Schools", "Solar_Panel", "Kitchen_Garden", "Rain_Water_Harvesting", "Incinerator"
	  config = {'A':[1, 1, 1, 1, 1], 'B':[1, 1, 0, 1, 1], 'C':[1, 1, 0, 1, 0]}
	  configWeightage = [0, 0.2, 0.3, 0.4, 0.1]
	  LocationList = ["Rural", "Urban"]
	  data = {}
	  for State in syntheticStates:
		vals = []
		for i in range(0, int(totalNumberOfRowsPerState)):
		  District_Name = chr(ord(ch) + i%26)
		  configValues = config[State]
		  Total_Number_of_Schools = configValues[0]
		  Solar_Panel = configValues[1]
		  Kitchen_Garden = configValues[2]
		  Rain_Water_Harvesting = configValues[3]
		  Incinerator = configValues[4]
		  val = (State, District_Name, Total_Number_of_Schools, Solar_Panel, Kitchen_Garden, Rain_Water_Harvesting,\
				 Incinerator, Academic_Year)
		  vals.append(val)
		  Academic_Year +=1
		
		data[State] = spark.createDataFrame(vals, requiredFields)
	  return data, config, configWeightage

	def generateSyntheticDataQ5():
	  requiredFields = ["State_Name", "Location", "Medical_Checkup", "Complete_Medical_Checkup", "Total_Number_of_Schools"]

	  realData = readAllDataForState("Karnataka") #This is just to get the column names/schema, could be any state
	  schema = realData.schema
	  pds = realData.toPandas()
	  syntheticData = spark.createDataFrame(pds, schema=schema)
	  syntheticData = syntheticData.select(requiredFields)
	  syntheticData = syntheticData.filter(syntheticData.State_Name == "")

	  southIndiaMedicalCheckupUnavailability = 0.7
	  totalNumberOfRowsPerState = 2 * 1000
	  syntheticStates = ["A", "B", "C"]

	  #Complete Medical Check-up percentage for rural and urban each state
	  config = {'A':{'Urban': 0.6, 'Rural': 0.4}, 'B':{'Urban': 0.3, 'Rural': 0.7}, 'C':{'Urban': 0.2, 'Rural': 0.8}}
	  data = {}

	  for State in syntheticStates:
		vals = []
		Location = "Rural"
		ruralPercentage = int(totalNumberOfRowsPerState*config[State]['Rural'])
		for i in range(0, ruralPercentage):
		  if i < (int) (ruralPercentage * (1-southIndiaMedicalCheckupUnavailability)):
			val = (State, Location, 1, 1, 1)
		  else:
			val = (State, Location, 0, 1, 1)
		  vals.append(val)

		Location = "Urban"
		urbanPercentage = int(totalNumberOfRowsPerState*config[State]['Urban'])
		for i in range(0, urbanPercentage):
		  if i < (int) (urbanPercentage * (1-southIndiaMedicalCheckupUnavailability)):
			val = (State, Location, 1, 1, 1)
		  else:
			val = (State, Location, 0, 1, 1)
		  vals.append(val)
		data[State] = spark.createDataFrame(vals, requiredFields)
		syntheticData = syntheticData.union(data[State])

	  return syntheticData, southIndiaMedicalCheckupUnavailability