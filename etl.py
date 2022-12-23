import webbrowser
from dagster import job
from extract1 import *
from transform1 import *
from load1 import *
from extract_pp import *
from transform_pp import *
from load_pp import *
from extract_WB import *
from transform_wb import *
from load_WB import *


@job
def etl():
    load_IRL_SH_TBL(
        stage_transformed_IRL_SH(
            transform_extracted_IRL_SH(
               stage_extracted_IRL_SH(
                   extract_IRL_SH()
            )
        )   
    )  
)    
    load_IRL_PP_TBL(
        stage_transformed_IRL_PP(
            transform_extracted_IRL_PP(
               stage_extracted_IRL_PP(
                   extract_IRL_PP()
            )
        )   
    )  
)                                     

    load_EU_SI_TBL(
        stage_transformed_EU_SI(
            transform_extracted_EU_SI(
                stage_extracted_EU_SI(
                    extract_EU_SI()
                )
            )
        )
    ) 