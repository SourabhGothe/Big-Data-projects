from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
import pandas as pd
from pyspark.sql.functions import regexp_replace

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

years = ["2017_2018", "2018_2019", "2019_2020"]
SouthStates = ["AndhraPradesh", "Karnataka", "Kerala", "TamilNadu", "Telangana"]
basePath = "/content/drive/Shareddrives/DataEngineeringProject/SouthDistricts/"

def state_urban_rural_school_dist(states, data, axes1 = None, index=0):
  #AndhraPradesh, school count urban, rural distribution
  answer = {}
  answer_print = {}
  answer_plotstate = {}
  for state in states:
    STdata = data[state]
    current_year = data[states[0]].first()['Academic_Year']
    STseldata = STdata.select("State_Name", "Location", "Total_Number_of_Schools")
    STAllRural = STseldata.select("State_Name", "Location", "Total_Number_of_Schools").where(col("Location") == "Rural")
    STAllUrban = STseldata.select("State_Name", "Location", "Total_Number_of_Schools").where(col("Location") == "Urban")

    STcnt = STseldata.groupBy("State_Name").sum("Total_Number_of_Schools").withColumnRenamed("sum(Total_Number_of_Schools)", "School_Count")

    STAllRuralGrouped = STAllRural.groupBy("State_Name").sum("Total_Number_of_Schools").withColumnRenamed("sum(Total_Number_of_Schools)", "Rural_School_Count")

    STAllUrbanGrouped = STAllUrban.groupBy("State_Name").sum("Total_Number_of_Schools").withColumnRenamed("sum(Total_Number_of_Schools)", "Urban_School_Count")\
                                                                                .withColumnRenamed("State_Name", "State")

    STAll = STAllRuralGrouped.join(STAllUrbanGrouped).where(STAllRuralGrouped.State_Name == STAllUrbanGrouped.State).drop("State").withColumnRenamed("State_Name", str(current_year))
    answer_plotstate[state] = STAll
    final=[]
    for i in STAll.collect():
      final.append(tuple(i))
    answer[state] = final
    Ruralcnt = final[0][1]
    Urbancnt = final[0][2]
    answer_print[state] = Urbancnt/Ruralcnt
  answer_print = dict(sorted(answer_print.items(), key=lambda item: item[1], reverse=True))
  if "A" not in states:
    allStatesData = [answer_plotstate[state] for state in states]
    allStatesData = unionAll(*allStatesData)
    plotallStatesData = allStatesData.toPandas()
    plotallStatesData.plot.bar(x=current_year, rot=0, ax=axes1[index])
  return answer_print, answer
  
def districts_highest_rural_schools(states, data):
  answer = {}
  consolidate = []  #use union method as sourabh later
  #all states top 10 districts with highest number of schools in rural area
  for state in states:
    STdata = data[state]
    STseldata = STdata.select("State_Name", "District_Name", "Location", "Total_Number_of_Schools")
    STMgtcnt = STseldata.groupBy("State_Name", "District_Name", "Location").sum("Total_Number_of_Schools")
    STtop = STMgtcnt.select("District_Name", "Location", "sum(Total_Number_of_Schools)").where(col("Location") == "Rural")
    STtop = STtop.orderBy(col("sum(Total_Number_of_Schools)").desc()).drop("Location")
    final=[]
    for i in STtop.take(10):
      final.append(tuple(i))
      consolidate.append(tuple(i))
    answer[state] = sorted(final) #For synthetic testing
  consolidate.sort(key = lambda x: x[1], reverse=True)
  q2_answer = []
  for i in range(0, 10):
    q2_answer.append(consolidate[i])
  return q2_answer, answer

def district_urban_rural_category_school_dist(states, data):
  answer = {}
  consolidate = [] #use union method as sourabh later
  # Create an empty RDD with empty schema
  for state in states:
    final=[]
    #Telangana, classification of schools based on school management in urban, rural across states
    STdata = data[state]
    STseldata = STdata.select("State_Name", "District_Name", "Location", "School_Management_Name", "School_Management_Id", "Total_Number_of_Schools")
    STMgtcnt = STseldata.groupBy("State_Name", "District_Name", "Location", "School_Management_Id").sum("Total_Number_of_Schools")
    #Private Unaided (Recognized) ->either consider this only for this question or take all private schools
    STtop = STMgtcnt.select("District_Name", "Location", "sum(Total_Number_of_Schools)").where((col("Location") == "Rural") & (col("School_Management_Id") == 5))
    STtop = STtop.orderBy(col("sum(Total_Number_of_Schools)").desc()).drop("Location")
    for i in STtop.take(10): #modified to 10, as top 10 can be from same state
      final.append(tuple(i))
      consolidate.append(tuple(i))
    answer[state] = sorted(final) #For synthetic testing
  consolidate.sort(key = lambda x: x[1], reverse=True)
  q3_answer = []
  for i in range(0, 10):
    q3_answer.append(consolidate[i])
  return q3_answer, answer

def my_autopct(pct):
    return ('%.2f' % pct) if pct > 2 else ''
def classification_school_management(states, data):
  answer_urban = {}
  answer_rural = {}
  for state in states:
    STdata = data[state]
    STseldata = STdata.select("State_Name", "District_Name", "Location", "School_Management_Name", "School_Management_Id", "Total_Number_of_Schools")
    STMgtcnt = STseldata.groupBy("State_Name", "Location", "School_Management_Name", "School_Management_Id").sum("Total_Number_of_Schools")
    STMgtcntUrban = STMgtcnt.select("State_Name", "Location", "sum(Total_Number_of_Schools)", "School_Management_Name", "School_Management_Id").where(col("Location") == "Urban")
    STMgtcntRural = STMgtcnt.select("State_Name", "Location", "sum(Total_Number_of_Schools)", "School_Management_Name", "School_Management_Id").where(col("Location") == "Rural")
    answer_urban[state] = STMgtcntUrban
    answer_rural[state] = STMgtcntRural
  allStatesDataUrban = [answer_urban[state] for state in states]
  allStatesDataUrban = unionAll(*allStatesDataUrban)
  allStatesDataUrban = allStatesDataUrban.groupBy("School_Management_Name", "School_Management_Id").sum("sum(Total_Number_of_Schools)").withColumnRenamed("sum(sum(Total_Number_of_Schools))", "Urban_school_count").drop("School_Management_Id")
  allStatesDataRural = [answer_rural[state] for state in states]
  allStatesDataRural = unionAll(*allStatesDataRural)
  allStatesDataRural = allStatesDataRural.groupBy("School_Management_Name", "School_Management_Id").sum("sum(Total_Number_of_Schools)").withColumnRenamed("sum(sum(Total_Number_of_Schools))", "Rural_school_count").drop("School_Management_Id")

  ploturbandf = allStatesDataUrban.toPandas()
  list = ploturbandf['School_Management_Name'].tolist()
  ploturbandf.plot.pie(labels=None, y='Urban_school_count', figsize=(11, 11), title='School management distribution in Urban areas', autopct=my_autopct).legend(loc='upper left', labels=list)
 
  plotruraldf = allStatesDataRural.toPandas()
  list = []
  list = plotruraldf['School_Management_Name'].tolist()
  plotruraldf.plot.pie(labels=None, y='Rural_school_count', figsize=(11, 11), title='School management distribution in Rural areas', autopct=my_autopct).legend(loc='upper left', labels=list)
  return

def self_sustain_scoreQ1(states, data):
  answer = {}
  answer_state = {}
  consolidate = []
  
  for state in states:
    final=[]
    STdata = data[state]
    current_year = data[states[0]].first()['Academic_Year']
    STseldata = STdata.select("State_Name", "District_Name", "Total_Number_of_Schools", "Solar_Panel", "Kitchen_Garden", "Rain_Water_Harvesting", "Incinerator")
    STwscoredata = STseldata.withColumn("SP_wAVG", f.expr("(Solar_Panel / Total_Number_of_Schools) * 0.2"))\
                                   .withColumn("KG_wAVG", f.expr("(Kitchen_Garden / Total_Number_of_Schools) * 0.3"))\
                                   .withColumn("RWH_wAVG", f.expr("(Rain_Water_Harvesting / Total_Number_of_Schools) * 0.4"))\
                                   .withColumn("I_wAVG", f.expr("(Incinerator / Total_Number_of_Schools) * 0.1"))
    STsum = STwscoredata.withColumn("Score_Wsum", f.expr("SP_wAVG + KG_wAVG + RWH_wAVG + I_wAVG"))
    STDistrictAvr = STsum.groupBy("State_Name", "District_Name").avg("Score_Wsum").withColumnRenamed("avg(Score_Wsum)", "WeightedAverage").cache()

    #In percentage
    STDistrictAvr = STDistrictAvr.withColumn("Sustainability_score", f.expr("WeightedAverage * 100")).drop("WeightedAverage")

    #for plot
    STstateAvr = STDistrictAvr.groupBy("State_Name").avg("Sustainability_score").withColumnRenamed("avg(Sustainability_score)", "Sustainability_score")
    answer_state[state] = STstateAvr
    STDistrictAvrSorted = STDistrictAvr.orderBy(col("Sustainability_score").desc()).drop("State_Name")

    for i in STDistrictAvrSorted.take(10): #top 10 can be from same state
      final.append(tuple(i))
      consolidate.append(tuple(i))
    answer[state] = sorted(final)

  consolidate.sort(key = lambda x: x[1], reverse=True)
  q1_answer = []
  for i in range(0, 10):
    q1_answer.append(consolidate[i])
  
  allStatesData = [answer_state[state] for state in states]
  allStatesData = unionAll(*allStatesData)
  return q1_answer, answer, allStatesData

def private_govt_self_sustain_score_dist_Q2(states, data, axes1 = None, index=0):
  answer = {}
  answer_plotstate = {}
  for state in states:
    STdata = data[state]
    current_year = data[states[0]].first()['Academic_Year']
    STseldata = STdata.select("State_Name", "District_Name", "Total_Number_of_Schools", "School_Management_Name", "School_Management_Id", "Solar_Panel", "Kitchen_Garden", "Rain_Water_Harvesting", "Incinerator")
    STwscoredata = STseldata.withColumn("SP_wAVG", f.expr("(Solar_Panel / Total_Number_of_Schools) * 0.2"))\
                                   .withColumn("KG_wAVG", f.expr("(Kitchen_Garden / Total_Number_of_Schools) * 0.3"))\
                                   .withColumn("RWH_wAVG", f.expr("(Rain_Water_Harvesting / Total_Number_of_Schools) * 0.4"))\
                                   .withColumn("I_wAVG", f.expr("(Incinerator / Total_Number_of_Schools) * 0.1"))
    STsum = STwscoredata.withColumn("Score_Wsum", f.expr("SP_wAVG + KG_wAVG + RWH_wAVG + I_wAVG"))
    STSchoolMgmtAvr = STsum.groupBy("State_Name", "School_Management_Id").avg("Score_Wsum").withColumnRenamed("avg(Score_Wsum)", "WeightedAverage").cache()


    #private assumption: 5 = private(unaided) 
    STAllPrivate = STSchoolMgmtAvr.select("State_Name", "School_Management_Id", "WeightedAverage").where(col("School_Management_Id") == 5)
    STAllPrivate = STAllPrivate.groupBy("State_Name").avg("WeightedAverage").withColumnRenamed("avg(WeightedAverage)", "PrivateWeightedAverage")

    #Aided #4= Government Aided
    STAllAided = STSchoolMgmtAvr.select("State_Name", "School_Management_Id", "WeightedAverage").where(col("School_Management_Id") == 4)
    STAllAided = STAllAided.groupBy("State_Name").avg("WeightedAverage").withColumnRenamed("avg(WeightedAverage)", "AidedWeightedAverage").withColumnRenamed("State_Name", "AidedState_Name")

    #Others 98 = Madarsa unrecognized 3= unrecognized 97 = Madarsa recognized (by Wakf board/Madarsa Board)
    STAllOthers = STSchoolMgmtAvr.select("State_Name", "School_Management_Id", "WeightedAverage")\
                                        .where((col("School_Management_Id") == 98) | (col("School_Management_Id") == 3) | (col("School_Management_Id") == 97))
    STAllOthers = STAllOthers.groupBy("State_Name").avg("WeightedAverage").withColumnRenamed("avg(WeightedAverage)", "OthersWeightedAverage").withColumnRenamed("State_Name", "OthersState_Name")

    #Govt: 1= department of education, 2 = Tribal Welfare Department, 93 = Jawahar Navodaya Vidyalaya 
    #92 = Kendriya Vidyalaya 90 = Social welfare Department 8=local body 94 = Sainik School 
    #6 = Other Govt. managed schools 96 = Central Tibetan School 101 = Other Central Govt. Schools
    #91 = ministry of labour #95 = railway school
    STAllGovt = STSchoolMgmtAvr.select("State_Name", "School_Management_Id", "WeightedAverage")\
                                       .where((col("School_Management_Id") == 1) | (col("School_Management_Id") == 2) |\
                                              (col("School_Management_Id") == 6)| (col("School_Management_Id") == 93) |\
                                              (col("School_Management_Id") == 8) | (col("School_Management_Id") == 94) |\
                                              (col("School_Management_Id") == 101) | (col("School_Management_Id") == 96) |\
                                              (col("School_Management_Id") == 92) | (col("School_Management_Id") == 90) |\
                                              (col("School_Management_Id") == 91) | (col("School_Management_Id") == 95))
    STAllGovt = STAllGovt.groupBy("State_Name").avg("WeightedAverage").withColumnRenamed("avg(WeightedAverage)", "GovtWeightedAverage").withColumnRenamed("State_Name", "GovtState_Name")
    STAll = STAllPrivate.join(STAllGovt).join(STAllAided).join(STAllOthers)\
                                        .withColumn("PrivateSelfSustainanceScore", col("PrivateWeightedAverage") * 100)\
                                        .withColumn("GovtSelfSustainanceScore", col("GovtWeightedAverage") * 100)\
                                        .withColumn("AidedSelfSustainanceScore", col("AidedWeightedAverage") * 100)\
                                        .withColumn("OthersSelfSustainanceScore", col("OthersWeightedAverage") * 100)\
                                        .drop("GovtState_Name").drop("GovtWeightedAverage").drop("PrivateWeightedAverage")\
                                        .drop("OthersState_Name").drop("AidedState_Name").drop("AidedWeightedAverage").\
                                        drop("OthersWeightedAverage").withColumnRenamed("State_Name", current_year)
    answer_plotstate[state] = STAll


    STAll = STAll.drop("State_Name")
    final=[]
    for i in STAll.collect():
      final.append(tuple(i))
    answer[state] = final

  allStatesData = [answer_plotstate[state] for state in states]
  allStatesData = unionAll(*allStatesData)
  plotallStatesData = allStatesData.toPandas()
  plotallStatesData.plot.bar(x=current_year, rot=0, ax=axes1[index])
  return answer