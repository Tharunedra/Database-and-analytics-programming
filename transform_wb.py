from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
  
#### TRANSFORM EU_SI ####
Transform_EU_SIDataFrame = create_dagster_pandas_dataframe_type(
    name="Transform_EU_SIDataFrame",
    columns=[
        PandasColumn.string_column("economy",non_nullable=False),
        PandasColumn.string_column("series", non_nullable=False),
        PandasColumn.string_column("Year", non_nullable=False),
        PandasColumn.float_column("Value", non_nullable=False) 
        
    ],
)

@op(ins={'start':In(None)},out=Out(Transform_EU_SIDataFrame))
def transform_extracted_EU_SI(start) -> Transform_EU_SIDataFrame:
    T_EU_SI = pd.read_csv("D:/DAP/ca samples/AUTO/EU_SI.csv", sep="\t")
    T_EU_SI['Year'].str.replace('YR','')
    T_EU_SI.dropna(how='any',inplace=True)
    return T_EU_SI

@op(ins={'T_EU_SI': In(Transform_EU_SIDataFrame)}, out=Out(None))
def stage_transformed_EU_SI(T_EU_SI):
    T_EU_SI.to_csv("D:/DAP/ca samples/AUTO/transformed_EU_SI.csv",sep="\t",index=False)
    
