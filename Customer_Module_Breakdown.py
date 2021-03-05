# Import the required Packages
import pandas as pd
import numpy as np
import pymssql
import sqlalchemy as sa

# Extract the DB server credentials from a local text file
with open('C:/Users/Sakthi/Desktop/Production-Data/Modulewise_Usage/script_db_credentials.txt') as f:
    lines = f.readlines()
server = lines[0].replace('\n', '')
username = lines[1].replace('\n', '')
password = lines[2].replace('\n', '')
testserver = lines[3].replace('\n', '')
testusername = lines[4].replace('\n', '')
testpassword = lines[5].replace('\n', '')

# Connect to the Production DB to extract Active User Count & Age & Module subscription details of the Customers
cnx = pymssql.connect(host=server,user=username,password=password,database='membership',port=1433)
cursor = cnx.cursor()
sql = "select t1.CompanyGUID, t1.CompanyName, t2.Name as Country, t1.ActiveUsers, t1.Age_Customer_InYrs from (select mc.CompanyGUID, mc.CompanyName +'_'+ mc.CompanyID as CompanyName, mc.CountryCode as CountryCode, count(mu.UserGUID) as ActiveUsers, round(convert(float, datediff(MM, mc.StatusDateTime,GETDATE()))/12,1) AS Age_Customer_InYrs  from Membership_User mu join Membership_Company mc on mc.CompanyGUID=mu.CompanyGUID where  mu.status=1 and mu.CompanyGUID in (select CompanyGUID from Membership_Company where status=1) group by mc.CompanyGUID, mc.CompanyName +'_'+ mc.CompanyID, mc.CountryCode, round(convert(float, datediff(MM, mc.StatusDateTime,GETDATE()))/12,1)) t1 left join Countries t2 on t1.CountryCode = t2.Code"
modsql = "select mc.CompanyGUID, dbo.GetModuleListByCompanyGUID(mc.CompanyGUID) AS ModuleName from membership..membership_company mc WHERE mc.Status = 1"
Active_Companies_df = pd.io.sql.read_sql(sql, cnx)
Modules_df = pd.io.sql.read_sql(modsql, cnx)
cnx.close()

Active_Companies_df = pd.merge(Active_Companies_df, Modules_df,how='left',on=['CompanyGUID'])

# Function to Compute the active users breakdown by the modules subscribed
def ModuleCount(id,name):
    mid=str(name)
    colname = mid+"_Count"
    cnx = pymssql.connect(host=server, user=username, password=password, database='membership')
    cursor = cnx.cursor()
    sql1 =  "SELECT CompanyGUID, count(*) as "+colname+" from Membership_User where Status=1 and CompanyGUID in (select CompanyGUID from Membership_Company where status=1) and UserGUID not in (select UserGUID from UserModuleAssign where ModuleID="+str(id)+") group by CompanyGUID"
    name = pd.io.sql.read_sql(sql1, cnx)
    cnx.close()
    for row in name.CompanyGUID:
        if not Active_Companies_df.ix[Active_Companies_df.CompanyGUID == row, 'ModuleName'].str.contains(mid).any():
            x = name.loc[:, 'CompanyGUID'] == row
            ind = x[x].index
            name.loc[ind[0], colname] = ""
    return name

# Pass the ModuleID & ModuleName as parameters to the function
eForm_df = ModuleCount(12004,'eForm')
eLeave_df = ModuleCount(12005,'eLeave')
ePayroll_df = ModuleCount(12029,'ePayroll')
eTimeclock_df = ModuleCount(12023,'eTimeclock')

# Merge all the individual module user count dataframe with the master dataframe
Active_Companies_df = pd.merge(Active_Companies_df, eForm_df,how='left',on=['CompanyGUID'])
Active_Companies_df = pd.merge(Active_Companies_df, eLeave_df,how='left',on=['CompanyGUID'])
Active_Companies_df = pd.merge(Active_Companies_df, ePayroll_df,how='left',on=['CompanyGUID'])
Active_Companies_df = pd.merge(Active_Companies_df, eTimeclock_df,how='left',on=['CompanyGUID'])
Active_Companies_df = Active_Companies_df.replace("",np.nan)

# Function to Create the columns for the modules subscribed
def ModuleColumnCreation(list):
    for i in range(0,len(list)):
        Active_Companies_df[list[i]] = ""
    return Active_Companies_df

# List that contains all the paid modules
PaidModuleslist = ['eLeave', 'eTimeclock','ePayroll','eForm','eBenefit','eSurvey','e360','eTimesheet','eAsset']

# Call the function by passing the paid modules list as argument to create the columns
ModuleColumnCreation(PaidModuleslist)

# Function to assign the status(1 or 0) based on the module subscription
def PaidModuleStatus(Active_Companies_df,PaidModuleslist):
    for i in range(0,len(Active_Companies_df)):
        for j in range(0,len(PaidModuleslist)):
            if Active_Companies_df['ModuleName'][[i]].str.contains(PaidModuleslist[j]).any():
                Active_Companies_df.loc[i, PaidModuleslist[j]] = 1
            else:
                Active_Companies_df.loc[i, PaidModuleslist[j]] = 0
    return Active_Companies_df

# Call the function by passing the Active_Company Dataframe & paid modules list as argument
PaidModuleStatus(Active_Companies_df,PaidModuleslist)

# Delete the ModuleName series from Active_Company Data frame
del Active_Companies_df['ModuleName']

# Create a connection engine to the test DB server and load the data into the table
engine = sa.create_engine('mssql+pymssql://'+str(testusername)+':'+str(testpassword)+'@'+str(testserver)+'/Data_Analytics')
Active_Companies_df.to_sql('Active_Company_Dictionary',con=engine,if_exists='replace',index=False)












