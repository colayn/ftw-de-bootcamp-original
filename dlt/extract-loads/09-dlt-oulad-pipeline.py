# dlt/pipeline.py
import dlt, pandas as pd
import os

# # https://archive.ics.uci.edu/dataset/9/auto+mpg
# @dlt.resource(name="cars")
# def mpg():
   
    
#     yield pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/mpg.csv").astype(str)
    
    # How to load a local CSV
    # Place file in staging\auto-mpg folder
    #ROOT_DIR = os.path.dirname(__file__)
    #STAGING_DIR = os.path.join(ROOT_DIR, "staging", "auto-mpg")
    #FILE_PATH = os.path.join(STAGING_DIR, "mpg.csv")
    # yield pd.read_csv(FILE_PATH).astype(str)
    
    # How to load an excel file
    #FILE_PATH = os.path.join(STAGING_DIR, "mpg.xlsx")
    #yield pd.read_excel(FILE_PATH).astype(str)


# # --- New newborn resource ---
# @dlt.resource(write_disposition="append", name="student_assessment")
# def studentAssessment():
#     ROOT_DIR = os.path.dirname(__file__)
#     STAGING_DIR = os.path.join(ROOT_DIR, "staging", "OULAD")
#     FILE_PATH = os.path.join(STAGING_DIR, "studentAssessment.csv")
#     yield pd.read_csv(FILE_PATH).astype(str)

# --- New newborn resource ---
@dlt.resource(write_disposition="append", name="vle")
def vle():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "OULAD")
    FILE_PATH = os.path.join(STAGING_DIR, "vle.csv")
    yield pd.read_csv(FILE_PATH).astype(str)


def run():
    p = dlt.pipeline(
        pipeline_name="09-dlt-oulad-pipeline",
        destination="clickhouse",
        dataset_name="grp5_oulad",
    )
    print("Fetching and loading...")
    
    # info2 = p.run(studentAssessment())
    # print("Records loaded:", info2)
    info2 = p.run(vle())
    print("Records loaded:", info2)


if __name__ == "__main__":
    run()