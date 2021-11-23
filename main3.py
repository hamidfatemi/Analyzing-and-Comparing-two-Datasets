import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from numpy import genfromtxt
import sqlalchemy as db
from sqlalchemy import create_engine , MetaData,Table,Integer,Float,Column,String,delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database


def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1)
    return data.tolist()
Base = declarative_base()

class Train_data(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Train_data'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False) 
    X = Column(Integer)
    Y1 = Column(Integer)
    Y2 = Column(Integer)
    Y3 = Column(Integer)
    Y4 = Column(Integer)
    
class Test_data(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Test_data'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False) 
    X = Column(Integer)
    Y = Column(Integer)
    
class Test_Table(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Test_Table'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False) 
    X = Column(Float)
    Y = Column(Float)
    y_Dev=Column(Float)
    y_No=Column(String)
    
class Ideal_data(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Ideal_data'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False) 
    X = Column(Integer)
    Y1 = Column(Integer)
    Y2 = Column(Integer)
    Y3 = Column(Integer)
    Y4 = Column(Integer)   
    Y5 = Column(Integer)
    Y6 = Column(Integer)
    Y7 = Column(Integer)
    Y8 = Column(Integer)  
    Y9 = Column(Integer)
    Y10 = Column(Integer)
    Y11 = Column(Integer)
    Y12 = Column(Integer)   
    Y13 = Column(Integer)
    Y14 = Column(Integer)
    Y15 = Column(Integer)
    Y16 = Column(Integer) 
    Y17 = Column(Integer)
    Y18 = Column(Integer)
    Y19 = Column(Integer)
    Y20 = Column(Integer)   
    Y21 = Column(Integer)
    Y22 = Column(Integer)
    Y23 = Column(Integer)
    Y24 = Column(Integer) 
    Y25 = Column(Integer)
    Y26 = Column(Integer)
    Y27 = Column(Integer)   
    Y28 = Column(Integer)
    Y29 = Column(Integer)
    Y30 = Column(Integer)
    Y31 = Column(Integer) 
    Y32 = Column(Integer)
    Y33 = Column(Integer)
    Y34 = Column(Integer)
    Y35 = Column(Integer)   
    Y36 = Column(Integer)
    Y37 = Column(Integer)
    Y38 = Column(Integer)
    Y39 = Column(Integer) 
    Y40 = Column(Integer)
    Y41 = Column(Integer)
    Y42 = Column(Integer)   
    Y43 = Column(Integer)
    Y44 = Column(Integer)
    Y45 = Column(Integer)
    Y46 = Column(Integer) 
    Y47 = Column(Integer)
    Y48 = Column(Integer)
    Y49 = Column(Integer)
    Y50 = Column(Integer)   

def exists(table) :
    try:
         engine.execute("SELECT * FROM " + table)
         return True
    except:
         return False
def main():        
    engine = db.create_engine('sqlite:///MyPyProject.db', echo = True)
    if not database_exists(engine.url):
        create_database(engine.url)
    print(database_exists(engine.url))
    meta = MetaData()
    Base.metadata.create_all(engine)
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    if exists('Test_table') :
        engine.execute('delete from "Test_table"')
        
    # deleting train table contents befor loading train.csv file into it for preventing duplicate values
    if exists('Train_data') :
        engine.execute('delete from "Train_data"')

    # loading train dataset into Train_data table
    engine.execute('delete from "Train_data"')
    try:
        file_name = "../train.csv"
        data = Load_Data(file_name) 
        for i in data:
            record = Train_data(**{
                'X' : i[0],
                'Y1' : i[1],
                'Y2' : i[2],
                'Y3' : i[3],
                'Y4' : i[4]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records

    
    except FileNotFoundError:
        print("train.csv File Not Found!")
        quit()
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() 

    # deleting Test_data table contents befor loading test.csv file into it for preventing duplicate values
    if exists('Test_data') :
        engine.execute('delete from "Test_data"')

    # loading the test.csv file into a Test_data table
    try:
        file_name = "../test.csv"
        data = Load_Data(file_name) 
        for i in data:
            record = Test_data(**{
                'X' : i[0],
                'Y' : i[1],
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
     except FileNotFoundError:
        print("test.csv File Not Found!")
        quit()
    except Exception as e:
        s.rollback() #Rollback the changes on error
        print(e)
    finally:
        s.close() 

    # deleting Ideal_data table contents befor loading ideal.csv file into it for preventing duplicate values
    if exists('Ideal_data') :
        engine.execute('delete from "Ideal_data"')

    # loading ideal.csv file into Ideal_data table
    try:
        file_name = "../ideal.csv"
        data = Load_Data(file_name) 
        for i in data:
            record = Ideal_data(**{
                'X' : i[0],
                'Y1' : i[1],
                'Y2' : i[2],
                'Y3' : i[3],
                'Y4' : i[4],
                'Y5' : i[5],
                'Y6' : i[6],
                'Y7' : i[7],
                'Y8' : i[8],
                'Y9' : i[9],
                'Y10' : i[10],
                'Y11' : i[11],
                'Y12' : i[12],
                'Y13' : i[13],
                'Y14' : i[14],
                'Y15' : i[15],
                'Y16' : i[16],
                'Y17' : i[17],
                'Y18' : i[18],
                'Y19' : i[19],
                'Y20' : i[20],
                'Y21' : i[21],
                'Y22' : i[22],
                'Y23' : i[23],
                'Y24' : i[24],
                'Y25' : i[25],
                'Y26' : i[26],
                'Y27' : i[27],
                'Y28' : i[28],
                'Y29' : i[29],
                'Y30' : i[30],
                'Y31' : i[31],
                'Y32' : i[32],
                'Y33' : i[33],
                'Y34' : i[34],
                'Y35' : i[35],
                'Y36' : i[36],
                'Y37' : i[37],
                'Y38' : i[38],
                'Y39' : i[39],
                'Y40' : i[40],
                'Y41' : i[41],
                'Y42' : i[42],
                'Y43' : i[43],
                'Y44' : i[44],
                'Y45' : i[45],
                'Y46' : i[46],
                'Y47' : i[47],
                'Y48' : i[48],
                'Y49' : i[49],
                'Y50' : i[50],
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except FileNotFoundError:
        print("ideal.csv File Not Found!")
        quit()
    except Exception as e:
        s.rollback() #Rollback the changes on error
        print(e)
    finally:
        s.close() 

    # defining a dataframe to put the Train_data table contents into it    
    Train_result = engine.execute('SELECT * FROM Train_data')
    Train_df=pd.DataFrame.from_records(Train_result,columns=["id","X","Y1","Y2","Y3","Y4"])
    Train_df=Train_df[["X","Y1","Y2","Y3","Y4"]]

    # defining a dataframe to put the Test_data table contents into it
    Test_result = engine.execute('SELECT * FROM Test_data')
    Test_df=pd.DataFrame.from_records(Test_result,columns=["id","X","Y"])
    Test_df=Test_df[["X","Y"]]

    # defining a dataframe to put the Ideal_data table contents into it
    Ideal_result = engine.execute('SELECT * FROM Ideal_data')
    Ideal_df=pd.DataFrame.from_records(Ideal_result,columns=["id","X","Y1","Y2","Y3","Y4","Y5","Y6","Y7","Y8","Y9","Y10","Y11","Y12",
    "Y13","Y14","Y15","Y16","Y17","Y18","Y19","Y20","Y21","Y22","Y23","Y24","Y25","Y26","Y27","Y28","Y29","Y30","Y31","Y32",
    "Y33","Y34","Y35","Y36","Y37","Y38","Y39","Y40","Y41","Y42","Y43","Y44","Y45","Y46","Y47","Y48","Y49","Y50"])

    Ideal_df=Ideal_df[["X","Y1","Y2","Y3","Y4","Y5","Y6","Y7","Y8","Y9","Y10","Y11","Y12",
    "Y13","Y14","Y15","Y16","Y17","Y18","Y19","Y20","Y21","Y22","Y23","Y24","Y25","Y26","Y27","Y28","Y29","Y30","Y31","Y32",
    "Y33","Y34","Y35","Y36","Y37","Y38","Y39","Y40","Y41","Y42","Y43","Y44","Y45","Y46","Y47","Y48","Y49","Y50"]]


    x=Train_df.X.values
    y1=Train_df.Y1.values
    y2=Train_df.Y2.values
    y3=Train_df.Y3.values
    y4=Train_df.Y4.values

    #calculating the diferences between train functions and ideal functions
    result_sigma=pd.DataFrame()
    for i in range(1,5):
        for j in range(1,51):
            result_sigma.loc["Y"+str(i),"Y"+str(j)]=((Train_df["Y"+str(i)]-Ideal_df["Y"+str(j)])**2).sum(axis=0)
        
    #selecting the 4 minimum of the above sum y-deviations and printing the 4 choosed ideal functions 
    result_minimum=pd.DataFrame()
    result_minimum=result_sigma.min(axis=1)
    result_min_index=result_sigma.idxmin(axis=1)
    col_list=result_min_index.values.tolist()
    col_list.insert(0,"X")
    new_df=Ideal_df[col_list]
        

    #map test points with the 4 selected ideal functions
    #if a test point mapped with more than one ideal function, the code below selects the ideal function with the smallest deviation
    #means I select only one ideal function for points that mapped with more than one ideal function
    #for the test points which not map with any ideal function I put 0 number under Dlta_y and number of ideal function
    final_result=pd.DataFrame()
    for i in range(len(Test_df)):
        y_test=Test_df.loc[i,"Y"] 
        x_test=Test_df.loc[i,"X"] 
        row_4=new_df.loc[new_df["X"]==x_test,[X for X in col_list if X!="X"]]
        temp_1=abs(row_4-y_test)
        result=abs(Train_df.loc[Train_df["X"]==x_test,[X for X in Train_df.columns.tolist() if X!="X"]]-row_4.values)
        final_result.loc[i,"X"]=x_test
        final_result.loc[i,"Y"]=y_test
        mask_result=(temp_1<(result.values*np.sqrt(2)))
        n =int(0)
        for col in mask_result.columns.tolist():
            if mask_result[col].values[0]:
                final_result.loc[i,'Delta_y']=temp_1.min(axis=1).item()
                minvalue=list(temp_1.idxmin(axis='columns'))
                final_result.loc[i,'Ideal_func_No']=minvalue[0]
                n+=1
    
        if n==0:
            final_result.loc[i,'Delta_y']=0
            final_result.loc[i,'Ideal_func_No']=0
     
    #putting the result of above function into answer table(Test_Table)
    #my answer table has 4 main columns that 2 of them are used to store X&Y of test points
    #and the 2 other colums are used to store the delta_y and number of mapped ideal function for each point
    #as I mentioned above for those test points which are not mapped with any ideal function I have used 0 number to put into the last two columns
    stmt="INSERT INTO Test_Table('X','Y','y_Dev','y_No') values(?,?,?,?)"
    for i in range(len(final_result)):
        engine.execute(stmt,final_result.iloc[i,0:4])
    
    #drawing the Y1 train function
    plt.plot(x,y1,'r.')
    plt.show()

    #drawing the Y2 train function
    plt.plot(x,y2,'r.')
    plt.show()

    #drawing the Y3 train function
    plt.plot(x,y3,'r.')
    plt.show()

    #drawing the Y4 train function
    plt.plot(x,y4,'r.')
    plt.show()

    #showing the contents of answer table(Test_Table) row by row
    Trow = engine.execute('select * from Test_Table')
    for row in Trow:
        print(row)

    #printing the 4 choosed ideal function name
    print('The selected Ideal functions are:',col_list[1:5])

if __name__ == '__main__':
    main()
