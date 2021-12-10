

from functools import reduce
from pyspark.sql.functions import col
from pyspark.sql.types import StructType 
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as psf
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import matplotlib.pyplot as plt
import pandas as pd


#Constant paths, variables, etc
years = ["2017_2018", "2018_2019", "2019_2020"]
SouthStates = ["AndhraPradesh", "Karnataka", "Kerala", "TamilNadu", "Telangana"]
basePath = "/content/drive/Shareddrives/DataEngineeringProject/SouthDistricts/"

# get all states to a single dataframe with facility data
def get_facilities(states = SouthStates, data = None):

  #columns = ["State_Name", "District_Name", "Location", "School_Management_Name", "Total_Number_of_Schools", "Functional_Electricity", "Solar_Panel", "Playground", "Library_or_Reading_Corner_or_Book_Bank", "Newspaper", "Kitchen_Garden", "Functional_Boy_Toilet", "Functional_Girl_Toilet", "Functional_Toilet_Facility", "Functional_Toilet_and_Urinal", "Functional_Drinking_Water", "Water_Purifier", "Rain_Water_Harvesting", "Handwash", "Incinerator", "Water_Tested", "WASH_Facility_Drinking_Water_Toilet_and_Handwash", "Ramps", "Medical_Checkup", "Complete_Medical_Checkup", "Internet", "Computer_Available"]
  columns = ["State_Name", "Total_Number_of_Schools", "Functional_Electricity", "Playground", "Library_or_Reading_Corner_or_Book_Bank", "Newspaper", "Functional_Boy_Toilet", "Functional_Girl_Toilet", "Functional_Toilet_Facility", "Functional_Urinal_Boy", "Functional_Urinal_Girl", "Functional_Urinal" , "Functional_Toilet_and_Urinal", "Functional_Drinking_Water", "Water_Purifier", "Handwash", "Ramps", "Internet", "Computer_Available"]

  #emptyDFInit = spark.sparkContext.emptyRDD().toDF(StructType([]))
  all_facilities = data

  #print(data)

  if all_facilities == None: # retrieve dara
    # get data for all states
    for state in states:
      currStateData = readAllDataForState(state).select([col for col in columns])
      
      if all_facilities == None:
        all_facilities = currStateData
      else:
        all_facilities = currStateData.union(all_facilities)
  else: # probably synthetic
    all_facilities = data.select([col for col in columns])

  #all_facilities.show(5)

  return all_facilities



def specially_abled_ranking(states = SouthStates, data = None):

  columns = ['State_Name', 'Total_Number_of_Schools', 'Ramps']
  answer = {}
  
  # get facility list of all states
  ramp_df = get_facilities(states, data).select([col for col in columns])
  
  # Subtracting ramp count from school count
  # greater number indicates lesser accessibility for differently abled
  ramp_df = ramp_df.withColumn('SchoolsWithoutRamp', (ramp_df.Total_Number_of_Schools - ramp_df.Ramps))

  ramp_columns = ['State_Name', 'Total_Number_of_Schools','SchoolsWithoutRamp']
  # print(ramp_df2.count())

  ramp_df2 = ramp_df.select([col for col in ramp_columns])

  state_wise_ramp = ramp_df2.groupBy("State_Name").agg({'Total_Number_of_Schools':'sum', 'SchoolsWithoutRamp':'sum'})

  # Subtracting ramp count from school count, and dividing by total number of schools
  # Converted to % after subtracting from 1 ====> 100% denotes full accessibility, 0% is worst
  state_wise_ramp = state_wise_ramp.withColumn('RampVsSchoolRatio', (100 * (1-state_wise_ramp['sum(SchoolsWithoutRamp)'] / state_wise_ramp['sum(Total_Number_of_Schools)'])).cast(IntegerType())).orderBy(col('RampVsSchoolRatio').desc())
  state_wise_ramp = state_wise_ramp.select(['State_Name', 'RampVsSchoolRatio'])
  #state_wise_ramp.show()

  for row in state_wise_ramp.rdd.collect():
    answer[row['State_Name']] = row['RampVsSchoolRatio']

  return state_wise_ramp, answer


def essentials_schoolper_state(states = SouthStates, data = None):

  columns = ['State_Name', 'Total_Number_of_Schools', 'Functional_Drinking_Water', 'Functional_Boy_Toilet', 'Functional_Girl_Toilet', \
             'Handwash', 'Functional_Electricity', 'Newspaper', 'Library_or_Reading_Corner_or_Book_Bank', 'Playground']
  answer = {}
  
  # get facility list of all states
  essential_df = get_facilities(states, data).select([col for col in columns])
  #print(essential_df.count())

  essential_df1 = essential_df.groupBy("State_Name").agg(psf.sum('Total_Number_of_Schools').alias('Total_Number_of_Schools'), \
                                                         psf.sum('Functional_Drinking_Water').alias('Functional_Drinking_Water'), \
                                                         psf.sum('Functional_Boy_Toilet').alias('Functional_Boy_Toilet'), \
                                                         psf.sum('Functional_Girl_Toilet').alias('Functional_Girl_Toilet'), \
                                                         psf.sum('Handwash').alias('Handwash'), \
                                                         psf.sum('Functional_Electricity').alias('Functional_Electricity'), \
                                                         psf.sum('Newspaper').alias('Functional_Newspaper'), \
                                                         psf.sum('Library_or_Reading_Corner_or_Book_Bank').alias('Library_or_Reading_Corner_or_Book_Bank'), \
                                                         psf.sum('Playground').alias('Playground'))
  
  #essential_df1.show()

  fields = essential_df1.schema.fields

  for col in fields:
    if str(col.dataType) is not "StringType":
      if col.name == 'Total_Number_of_Schools':
        continue
      
      essential_df1 = essential_df1.withColumn(col.name, (100 * \
                                                     (1 - ((essential_df1['Total_Number_of_Schools'] - essential_df1[col.name]) / essential_df1['Total_Number_of_Schools']))).cast(IntegerType()))


  essential_df1 = essential_df1.drop('Total_Number_of_Schools')
  #essential_df1.show()

  answer['names'] = essential_df1.columns
  answer['values'] = [list(row) for row in essential_df1.collect()]

  return essential_df1, answer
# Investigating availability of Essential, Advanced Facilities
# c. Give the percentage of schools per state which provide advanced facilities like internet, computer

def advFacilities_schoolper_state(states = SouthStates, data = None):

  columns = ['State_Name', 'Total_Number_of_Schools', 'Internet', 'Computer_Available']
  answer = {}
  
  # get facility list of all states
  df = get_facilities(states, data).select([col for col in columns])
  df = df.groupBy("State_Name").agg(psf.sum('Total_Number_of_Schools').alias('Total_Number_of_Schools'), \
                                                         psf.sum('Internet').alias('Internet'), \
                                                         psf.sum('Computer_Available').alias('Computer_Available'))
  
  #df.show()

  fields = df.schema.fields

  for col in fields:
    if str(col.dataType) is not "StringType":
      if col.name == 'Total_Number_of_Schools':
        continue
      
      df = df.withColumn(col.name, (100 * (1 - (df['Total_Number_of_Schools'] - df[col.name]) / df['Total_Number_of_Schools'])).cast(IntegerType()))

  df = df.drop('Total_Number_of_Schools')
  #df.show()

  for row in df.rdd.collect():
    answer[row['State_Name']] = [row['Internet'], row['Computer_Available']]

  return df, answer

# get all states to a single dataframe with facility data
def get_medical(states = SouthStates, data = None):

  columns = ["State_Name", "Total_Number_of_Schools", "Location", "Medical_Checkup", "Complete_Medical_Checkup"]

  #emptyDFInit = spark.sparkContext.emptyRDD().toDF(StructType([]))
  medical_facilities = data

  if data == None: # retrieve dara
    # get data for all states
    for state in states:
      currStateData = readAllDataForState(state).select([col for col in columns])
      
      if medical_facilities == None:
        medical_facilities = currStateData
      else:
        medical_facilities = currStateData.union(medical_facilities)
  else: # probably synthetic
    medical_facilities = data.select([col for col in columns])
    
  #medical_facilities.show(5)

  return medical_facilities

# Investigate availability of Medical care
# a. Get the percentage of schools lacking medical check-up for south India
def getSchoolsLackingMedical(states = SouthStates, data = None):

  columns = ["State_Name", "Total_Number_of_Schools", "Medical_Checkup"] # Medical_Checkup is inclusive of Complete_Medical_Checkup
  answer = {}

  medi_df = get_medical(states, data).select([col for col in columns])
  medi_df = medi_df.groupBy("State_Name").agg(psf.sum('Total_Number_of_Schools').alias('Total_Number_of_Schools'), \
                                                         psf.sum('Medical_Checkup').alias('Medical_Checkup'))
  
  #medi_df.show()
  medi_df = medi_df.withColumn("No_Medical_%", \
                               (100 * ((medi_df['Total_Number_of_Schools'] - medi_df["Medical_Checkup"]) / medi_df['Total_Number_of_Schools'])) \
                               .cast(IntegerType())).orderBy(col('No_Medical_%').desc())
  medi_df = medi_df.drop('Total_Number_of_Schools', 'Medical_Checkup')
  #medi_df.show()
  
  for row in medi_df.rdd.collect():
    answer[row['State_Name']] = row['No_Medical_%']
  
  return medi_df, answer
# Investigate availability of Medical care
# b. Get the percentage of schools having complete medical check-up for south India and segregate it based on rural, urban category
def getUrbanRuralCompleteMedical(states = SouthStates, data = None):

  columns = ["Total_Number_of_Schools", 'Location', "Complete_Medical_Checkup"]
  answer = {}

  medi_df = get_medical(states, data).select([col for col in columns])
  #medi_df.show(5)
  medi_df = medi_df.groupBy("Location").agg(psf.sum('Total_Number_of_Schools').alias('Total_Number_of_Schools'), \
                                                         psf.sum('Complete_Medical_Checkup').alias('Complete_Medical_Checkup'))
  
  #medi_df.show()
  medi_df = medi_df.withColumn("Complete_Medical_%", \
                               (100 * (medi_df["Complete_Medical_Checkup"] / medi_df['Total_Number_of_Schools'])) \
                               .cast(IntegerType())).orderBy(col('Complete_Medical_%').desc())
  medi_df = medi_df.drop('Total_Number_of_Schools', 'Complete_Medical_Checkup')
  #medi_df.show()
  
  for row in medi_df.rdd.collect():
    answer[row['Location']] = row['Complete_Medical_%']

  return medi_df, answer
def plot_UrbanRuralCompleteMedical(states = SouthStates, data = None):
  medi_df, medi_dict = getUrbanRuralCompleteMedical(states, data)
  
  medi_df = medi_df.withColumn("No Complete Medical Care", (100 - medi_df['Complete_Medical_%'])).withColumnRenamed("Complete_Medical_%", "Complete Medical Care Available")
  mediPlot = medi_df.withColumnRenamed("Location", "2019-2020").toPandas()
  mediPlot["Complete Medical Care Available"]=pd.to_numeric(mediPlot["Complete Medical Care Available"])
  mediPlot["No Complete Medical Care"]=pd.to_numeric(mediPlot["No Complete Medical Care"])

  axes = mediPlot.plot.bar(x="2019-2020", rot=0, stacked = True)
  axes.legend(loc='upper right', bbox_to_anchor =(1.25, 1.25))
  axes.set_ylabel("Urban rural distribution for Complete Medical check up")

# Investigate availability of Medical care
# c. Plot the graph showing the state wise comparison
def getSchoolsHavingMedical(states = SouthStates, data = None):

  columns = ["State_Name", "Total_Number_of_Schools", "Medical_Checkup"] # Medical_Checkup is inclusive of Complete_Medical_Checkup
  answer = {}

  medi_df = get_medical(states, data)
  medi_df = medi_df.groupBy("State_Name").agg(psf.sum('Total_Number_of_Schools').alias('Total_Number_of_Schools'), \
                                                         psf.sum('Medical_Checkup').alias('Medical_Checkup'))
  
  #medi_df.show()
  medi_df = medi_df.withColumn("Medical_Available_%", \
                               (100 * (medi_df["Medical_Checkup"] / medi_df['Total_Number_of_Schools'])) \
                               .cast(IntegerType())).orderBy(col('Medical_Available_%').desc())
  medi_df = medi_df.drop('Total_Number_of_Schools', 'Medical_Checkup')
  #medi_df.show()

  for row in medi_df.rdd.collect():
    answer[row['State_Name']] = row['Medical_Available_%']

  return medi_df, answer

def plot_SchoolsLackingMedical(states, data):
  medi_df, medi_dict = getSchoolsLackingMedical(states, data)
  
  medi_df = medi_df.withColumn("Medical_%", (100 - medi_df['No_Medical_%'])).withColumnRenamed("Medical_%", "Medical Care Available")\
  .withColumnRenamed("No_Medical_%", "No Medical Care")
  mediPlot = medi_df.withColumnRenamed("State_Name", "2019-2020").toPandas()
  mediPlot["No Medical Care"]=pd.to_numeric(mediPlot["No Medical Care"])
  mediPlot["Medical Care Available"]=pd.to_numeric(mediPlot["Medical Care Available"])

  axes = mediPlot.plot.bar(x="2019-2020", rot=0, stacked = True)
  axes.set_ylabel("Schools lacking Medical Check up")

def plot_SchoolsHavingMedical(states = SouthStates, data = None):
  medi_df, medi_dict = getSchoolsHavingMedical(states, data)
  
  medi_df = medi_df.withColumn("No Medical Care", (100 - medi_df['Medical_Available_%'])).withColumnRenamed("Medical_Available_%", "Medical Care Available")
  mediPlot = medi_df.withColumnRenamed("State_Name", "2019-2020").toPandas()
  mediPlot["Medical Care Available"]=pd.to_numeric(mediPlot["Medical Care Available"])
  mediPlot["No Medical Care"]=pd.to_numeric(mediPlot["No Medical Care"])

  axes = mediPlot.plot.bar(x="2019-2020", rot=0, stacked = True)
  axes.legend(loc='upper right', bbox_to_anchor =(1.25, 1.25))
  axes.set_ylabel("Schools having Medical Check up")
  
def plot_LibraryAvailability(states = SouthStates, data = None):
  df = essentials_schoolper_state(states, data)[0]['State_Name', 'Library_or_Reading_Corner_or_Book_Bank']
  
  df = df.withColumn("No Library", (100 - df['Library_or_Reading_Corner_or_Book_Bank']))
  mediPlot = df.withColumnRenamed("State_Name", "2019-2020").toPandas()
  mediPlot["No Library"]=pd.to_numeric(mediPlot["No Library"])
  mediPlot["Library_or_Reading_Corner_or_Book_Bank"]=pd.to_numeric(mediPlot["Library_or_Reading_Corner_or_Book_Bank"])

  axes = mediPlot.plot.bar(x="2019-2020", rot=0, stacked = True)
  axes.legend(loc='upper right', bbox_to_anchor =(1.25, 1.25))
  axes.set_ylabel("Schools having Library")
