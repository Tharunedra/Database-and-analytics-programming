from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"

### WORLD BANK WEB API DATA ###
##DATAFRAME  ##

EU_SIDataFrame = create_dagster_pandas_dataframe_type(
    name="EU_SIDataFrame",
    columns=[
        PandasColumn.string_column("economy",non_nullable=False),
        PandasColumn.string_column("series", non_nullable=False),
        PandasColumn.string_column("Year", non_nullable=False),
        PandasColumn.float_column("Value", non_nullable=False)     
    ],
)

### EXTRACT EU_SI DATA###

@op(ins={'start': In(bool)}, out=Out(EU_SIDataFrame))
def extract_EU_SI(start) -> EU_SIDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["DAPGRPM_database"]
    EU_SI = pd.DataFrame(db.EU_socioeconomic_indicators.find({}))
    EU_SI.drop(
        columns=['_id'],
        axis=1,
        inplace=True
    )
    EU_SI.dropna(how='any',inplace=True)
    conn.close()
    return EU_SI
@op(ins={'EU_SI': In(EU_SIDataFrame)}, out=Out(None))
def stage_extracted_EU_SI(EU_SI):
    EU_SI.to_csv("D:\DAP\ca samples\AUTO\EU_SI.csv",index=False,sep="\t")
