from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd

postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/postgres"


## LOAD EU_SI TABLE TO POSTGRESQL##        
@op(ins={'start': In(None)},out=Out(bool))
def load_EU_SI_TBL(start):
    logger = get_dagster_logger()
    WB_DATA = pd.read_csv("D:/DAP/ca samples/AUTO/transformed_EU_SI.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("""DROP TABLE IF EXISTS public.WB_DATA;
                          CREATE TABLE IF NOT EXISTS public."WB_DATA"
                          (
                            "economy" text COLLATE pg_catalog."default",
                            "series" text COLLATE pg_catalog."default",
                            "Year" text COLLATE pg_catalog."default",
                            "Value" double precision
                            );""")
        rowcount = WB_DATA.to_sql(
            name="WB_DATA",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
        
