import glob
import time
import PySimpleGUI as sg
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import dask.dataframe as dd

#folder Input
def folderinput():
    while True:
        #Gets the folder path with the csv files
        data_folder = sg.PopupGetFolder('Select folder containing flight data files',default_path = 'D:\\Year 2\\Programming\\Coursework\\dataverse_files\\Years',)  
        if data_folder == (''):
            print('Please enter a file path')
        else:
            data_folder = data_folder.replace('/','\\')
            break
    return data_folder
data_folder= folderinput()

#Return a list of filenames with the csv extenstion in the folder
all_files = glob.glob(data_folder+"\\"+"*.csv")

#Reading all csv files and converting them to a pandas dataframe
start_time =  time.time()
dask_df = dd.read_csv(all_files ,
                        assume_missing=True,
                        dtype={'CancellationCode': 'object'})
df = dask_df.compute()

   
#Removes columns with 90% null values
df = df.drop(df.columns[df.isnull().mean()>0.90],axis=1)


#Removing columns 
def deletecol(df,x):
    del df[x]

deletecol(df,'TaxiIn')
deletecol(df,'TaxiOut')


#Convert all the NULL valued rows for delays to the average
def convert_null_to_avg(colname):
    x = df[colname].mean()
    df[colname].fillna(int(x),inplace=True)
    
convert_null_to_avg('ArrDelay')
convert_null_to_avg('DepDelay')


#Drop rows with null values in Arrival time or Departure Time
df = df.dropna(subset=['ArrTime', 'DepTime'])


def categorize_time(x):
    if (0000<= x <= 800):
        return 'Early Morining'  
    if(800<x<1200):
        return "Morning"
    if(1200 <= x < 1500):
        return "Afternoon"
    if(1500 <= x <1900):
        return "Evening"
    return 'Night'

df['Arrival_period'] = df['ArrTime'].apply(categorize_time)
df['Departure_period'] = df['DepTime'].apply(categorize_time)
df['PlaneDelay'] = df['ArrDelay'] + df['DepDelay'] 
Delay_time_arr = df.groupby('Arrival_period').mean()['PlaneDelay'].to_frame()
Delay_time_dep = df.groupby('Departure_period').mean()['PlaneDelay'].to_frame()
Delay_day = df.groupby('DayOfWeek').mean()['PlaneDelay'].to_frame()
Delay_month = df.groupby("Month").mean()['PlaneDelay'].to_frame()


months = {1.0: 'January' , 2.0: 'Feburary' , 3.0: 'March' , 4.0 : 'April' , 5.0:'May',
        6.0:'June', 7.0:'July' , 8.0:'August' , 9.0:'September' ,10.0:'October', 11.0:'November' ,12.0: 'December'}

Days = {1.0:'Monday' , 2.0:'Tuesday' , 3.0:'Wednesday' ,4.0: 'Thursday', 
        5.0:'Friday' , 6.0:'Saturday' ,7.0:'Sunday'}

Delay_month.index = Delay_month.index.map(months)
Delay_day.index = Delay_day.index.map(Days)

#Sort the delays from biggest to smallest for each category
def sorting(df):
    df = df.sort_values(by=['PlaneDelay'])
    return df
Delay_day = sorting(Delay_day)
Delay_month = sorting(Delay_month)


####################  Plotting graphs  ####################

sns.barplot(data = Delay_day , x = 'PlaneDelay' , y = Delay_day.index , palette = 'rocket')
plt.title('Average Plane Delay by days')
plt.xlabel('Plane Delay in Minutes')
plt.ylabel('Days')
plt.show()

sns.barplot(data = Delay_month , x = 'PlaneDelay' , y = Delay_month.index , palette = 'rocket' )
plt.title('Average Plane Delay by Months')
plt.xlabel('Plane Delay in Minutes')
plt.ylabel('Months')
plt.show()


lineplot = sns.lineplot(data = Delay_time_dep , x=Delay_time_arr.index , y='PlaneDelay' ,
                    color='blue' )
lineplot.legend(['Departure Delay'], loc="upper left")
lineplot.set(xlabel='Time of Day' , ylabel="Flight Delay in Minutes" )
boxplot = lineplot.twiny()
boxplot = sns.barplot(data = Delay_time_arr , x=Delay_time_arr.index , y='PlaneDelay' , alpha = 0.6  , ax = boxplot)
#boxplot.legend([])
boxplot.set(xlabel='' , xticks=[])
#boxplot.legend(['Arrival Delay'], loc="upper right")
boxplot.grid(False) 
plt.title('Plane Delays according to time period of a day')
plt.show()


print("Part one of the question done by: ",time.time() - start_time)


####################### Question 2 ##################

def plane_data_csv_input():
    while True:
        file =  sg.popup_get_file("Select Plane-Data csv file location",
                                  title ='My File Browser',
                                  default_path ='D:\\Year 2\\Programming\\Coursework\\dataverse_files\\plane-data.csv' ,
                                  file_types =(("ALL CSV Files", "*.csv"),  ))
        if file != (''):
            break
        sg.popup_error(' Please enter a file path ')
    return file

#Run the FileInput GUI to get user to locate the plane-data csv
plane_csv = plane_data_csv_input()

#Reads the csv into a variable
ddf_plane_data = dd.read_csv(plane_csv, dtype={'year':'object'})


#Merging the plane_data dataframe and the main dataframe
joined1  = df.merge(ddf_plane_data[['tailnum' , 'year']].compute(),
                    how='left' ,left_on=['TailNum'], right_on='tailnum' )

#Remove all rows that has null valued tailnum
#joined1['year'] = joined1['year'].dropna()
#joined1['tailnum'] =joined1['tailnum'].dropna()
#joined1['year'].isnull().sum()

joined1 = joined1[joined1['year'].str.contains('None|0') == False]

#Calculates the age of the plane by taking the year the plane was flown against it was made
joined1['plane_age'] = joined1['Year'].astype(int) - joined1['year'].astype(int)
joined1['PlaneDelay'] = joined1['PlaneDelay']/60

scatplot = sns.scatterplot(data = joined1 , x='plane_age' , y='PlaneDelay')
scatplot.set(title="Association between plane's age & Delay Time", xlabel = 'Age of Plane',ylabel = 'Delay in Hours')
plt.show()

####################### Question 3 #####################
# #folder Input
# def airports_csv_input():
#     while True:
#         #Gets the folder path with the csv files
#         data_folder = sg.PopupGetFolder('Select folder with data files',
#                     default_path = 'D:\\Year 2\\Programming\\Coursework\\dataverse_files\\airports.csv',)  
#         if data_folder == (''):
#             print('Please enter a file path')
#         else:
#             data_folder = data_folder.replace('/','\\')
#             break
#     return data_folder

# airports_csv= airports_csv_input()
# airport_data = dd.read_csv(airports_csv).compute()


# joined1 = df.merge(airport_data[['iata', 'city']],
#                     how = 'left' , left_on=['Origin'], right_on='iata')

joined1['flight'] = joined1['Origin'] + '-' + joined1['Dest']
joined1['DATE'] = pd.to_datetime(joined1[['Year', 'Month']].assign(DAY=1))
joined1['date'] = joined1['DATE'].apply(lambda x: x.strftime('%Y-%m'))

#joined1 = joined1[joined1['Month']<=4.0]

tmp_group1 = joined1.groupby(['date', 'Origin','Dest'])#.agg({'Dest':pd.Series.nunique})


#tmp_group2 = joined1.groupby(['flight']).agg({'flight':pd.Series.nunique})

tmp_group1.reset_index(inplace=True)

# Calculate the mean number of destinations for all origins per year
df_mean_num_dest = tmp_group1.groupby(tmp_group1.index).agg({'Dest': 'mean'})

sns.barplot(x=df_mean_num_dest.index,y=df_mean_num_dest.Dest)

