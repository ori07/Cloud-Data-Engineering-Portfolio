import numpy as np
import pandas as pd


# Flag to enrich the dataset
def flag_df(df: pd.DataFrame):
    # Flag the data with for business logic
    df_res = df.copy()
    condition_list = [
        ((df_res["Quantity"] == 0) | (df_res["UnitPrice"] < 0)),
        ((df_res["Quantity"] < 0) & (df_res["UnitPrice"] > 0.0)),
        ((df_res["Quantity"] > 0) & (df_res["UnitPrice"] == 0.0)),
    ]
    choice_list = ["Anomaly", "Refund", "Promotion/Gift"]
    df_res["Flag"] = np.select(condition_list, choice_list, default="Standard")
    return df_res
