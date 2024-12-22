
import my_ds_babel
import pandas as pd
from io import StringIO
import warnings

def my_m_and_a(content_database_1, content_database_2, content_database_3):
    df1 = pd.read_csv(content_database_1)
    df2 = pd.read_csv(content_database_2, sep=";", header=None, names=["Age", "City", "Gender", "Name", "Email", "Country"])
    df3 = pd.read_csv(content_database_3, sep="\t", skiprows=1, names=["Gender", "Name", "Email", "Age", "City", "Country"])
    gender = {"0": "Male", "1": "Female", "F": "Female", "M": "Male"}
    #Cleaning df1
    df1["Gender"] = df1["Gender"].replace(gender)
    df1["FirstName"] = df1["FirstName"].str.replace(r"\W", "", regex=True).str.title()
    df1["LastName"] = df1["LastName"].str.replace(r"\W", "", regex=True).str.title()
    df1["Email"] = df1["Email"].str.lower()
    df1["City"] = df1["City"].str.replace("_", "-").str.title()
    df1["Country"] = "USA"
    df1.drop(columns=["UserName"], inplace=True)
    #Cleaning df2
    df2["Gender"]=df2["Gender"].replace(gender)
    df2["Age"] = df2["Age"].str.replace(r"\D", "", regex=True)
    df2["City"]=df2["City"].str.title()
    df2[["FirstName", "LastName"]]=df2["Name"].str.split(expand=True)
    df2["FirstName"]=df2["FirstName"].str.replace(r"\W", "", regex=True).str.title()
    df2["LastName"]=df2["LastName"].str.replace(r"\W", "", regex=True).str.title()
    df2.drop(columns=["Name"], inplace=True)
    df2["Email"]=df2["Email"].str.lower()
    df2["Country"]="USA"
    #Cleaning df3
    df3["Gender"] = df3["Gender"].str.replace(r"character_|string_|boolean_", "", regex=True).replace(gender)
    df3["Age"] = df3["Age"].str.replace("interger_","").str.replace(r"\D", "", regex=True)
    df3["City"] = df3["City"].str.replace("string_", "").str.replace("_", "-").str.title()
    df3[["FirstName", "LastName"]]=df3["Name"].str.split(expand=True)
    df3["FirstName"]=df3["FirstName"].str.replace("string","").str.replace(r"\W", "", regex=True).str.title()
    df3["LastName"]=df3["LastName"].str.replace(r"\W", "", regex=True).str.title()
    df3.drop(columns=["Name"], inplace=True)
    df3["Email"]=df3["Email"].str.replace("string_", "").str.lower()
    df3["Country"]="USA"
    merged_df = pd.concat([df1, df2, df3], ignore_index=True)
    final_schema = ["Gender", "FirstName", "LastName", "Email", "Age", "City", "Country"]
    merged_df = merged_df.reindex(columns=final_schema)
    merged_df["FirstName"] = merged_df["FirstName"].astype(str)
    merged_df["LastName"] = merged_df["LastName"].astype(str)
    merged_df["Age"] = merged_df["Age"].astype(str)
    return merged_df


warnings.filterwarnings("ignore", message="The spaces in these column names will not be changed.")
content_database_1 = './customers1.csv'
content_database_2 = './customers2.csv'
content_database_3 = './customers3.csv'
merged_csv = my_m_and_a(content_database_1, content_database_2, content_database_3)
print(merged_csv)
my_ds_babel.csv_to_sql(StringIO(merged_csv.to_csv(index=False)), 'plastic_free_boutique.sql', 'customers')

