import pandas as pd
import logging

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def clean_description_column(df):
    '''
    Clean and normalize the Description column by removing special characters and converting to lowercase.
    '''
    df["Description"] = (
        df["Description"]
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^a-z0-9\s]", "", regex=True)  # only letter and numbers
    )

    return df

def clean_country_column(df):
    '''
    Normalize and map country names to reduce duplicates with different spellings or formats.
    '''
    country_map = {
        "united kingdom": "uk",
        "u.k.": "uk",
        "uk": "uk",
        "united  kingdom": "uk",
        "usa": "united states",
        "united states of america": "united states",
        "eire": "ireland"
    }

    df["Country"] = (
        df["Country"]
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .replace(country_map)
    )

    return df

#Read data with schema
dtypes={"InvoiceNo":str,"StockCode":str,"Description":str,"Quantity":int,"UnitPrice":float,"CustomerID":str,"Country":str}
parse_dates = ["InvoiceDate"]

raw_df=pd.read_csv("cleaning_and_quality/e_commerce/data/commerce_data.csv",
                   encoding="ISO-8859-1",
                   header=0,
                   dtype=dtypes, 
                   parse_dates=parse_dates)


#Check if data read 
logging.info("Data read completed.\n")
logging.info(f"{raw_df.head(10)}\n") 

#Check data types
logging.info(f"Data types\n: {raw_df.dtypes}\n")

#Check duplicate rows
logging.info(f"Duplicate row count: {raw_df.duplicated().sum()}\n")

#Drop duplicates
raw_df = raw_df.drop_duplicates()
logging.info("Duplicates removed.\n")


'''
#for debug: check if every value belongs to same datatype in columns
logging.info(raw_df["InvoiceNo"].apply(type).value_counts()) 
logging.info(raw_df["StockCode"].apply(type).value_counts())
logging.info(raw_df["Description"].apply(type).value_counts())
logging.info(raw_df["Quantity"].apply(type).value_counts())
logging.info(raw_df["UnitPrice"].apply(type).value_counts())
logging.info(raw_df["CustomerID"].apply(type).value_counts())
logging.info(raw_df["Country"].apply(type).value_counts())
'''

#Check nulls
logging.info(f"Null values before cleaning:\n {raw_df.isnull().sum()} \n")

#Drop rows with empty description
raw_df = raw_df.dropna(subset=["Description"])
logging.info("Empty description cleanup completed.\n")

#Fill customer_id with 'guest' where it is null. Represents customers who do not log in with a membership
raw_df["CustomerID"] = raw_df["CustomerID"].fillna("guest")
logging.info("CustomerID fill completed.\n")

#Categorical cleaning
clean_description_column(raw_df)
clean_country_column(raw_df)
logging.info("Categorical cleanup completed.\n")

#Feature Engineering 
raw_df["is_return"] = raw_df["Quantity"] < 0
raw_df["TotalPrice"] = (raw_df["Quantity"] * raw_df["UnitPrice"]).round(2)
logging.info("Feature columns added.\n")

#Final Validation
logging.info(f"Zero quantity but priced rows: {raw_df[(raw_df["Quantity"] == 0) & (raw_df["UnitPrice"] > 0)]}\n")
logging.info(f"Remaining null values:\n {raw_df.isnull().sum()}\n")
logging.info(f"Unique countries after cleaning:\n {raw_df["Country"].unique()}\n")

#Export cleaned data
raw_df.to_csv("cleaning_and_quality/e_commerce/data/cleaned_ecommerce_data.csv", index=False)
logging.info("Clean data exported.\n")

