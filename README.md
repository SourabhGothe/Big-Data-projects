#Big-Data-Mini-Project

This prject is organized in 3 notebooks,

1. projectBase_Master.ipynb :
      1. Sets up the spark environment using resources from '/content/drive/Shareddrives/DA231-2021-Aug-Public/spark-3.0.3-bin-hadoop2.7.tgz'
      2. Imports all the methods from SyntheticDataGeneration.py, QuerySet1.py, QuerySet2.py which contain the main queries for the problem
      3. This notebook works like a driver code for all the problem statements and outputs the plots/text
      4. This notebook answeres total 5 Main Questions with sub-questions in it

2. VerificationNValidation.ipynb :
      1. This notebook verifies all the methods implemented from SyntheticDataGeneration.py, QuerySet1.py, QuerySet2.py which contain the main queries for the problem
      2. This notebook generates the synthetic data for each problem statement and verifies its solution, prints success if values are matching correctly
      3. End of this notebook also shows the scalability of few queries on varying data sizes

3. Correlation_Identification.ipynb :
      1. This Notebook, takes the pass percentage of each district in Karnataka for 2017, 2018, 2019
      2. Find the correlation with facilities and the performance of the district
      3. Finally plots the correlation of library availability and hygiene score with pass percentage (since postively correlated)

The Final Consolidated Analysis can be found in below file,
Educational_Infrastructure_Result_Analysis.pdf

To run the analysis, below things must be ensured
1. Data Folder :  Unzip "SouthDistricts.zip" and save as "SouthDistricts"
        Must point the "basePath" variable to where the "SouthDistricts" folder is present (In 4th cell)

2. Main Queries file :
        Must contain 3 python files --> SyntheticDataGeneration.py, QuerySet1.py, QuerySet2.py which contain the main queries for the problem
        
3. Please ensure spark setup is properly done and run to obtain all the results sucessfully
        
  
As part of course  DA231 at Indian Institute of Science

