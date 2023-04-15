from IndeedClean import *
import pandas as pd
import sys
import datetime
import pickle

date = datetime.date.today()
file = sys.argv[1]

def split_by_age(df, age_col="post_age"):
    """Split dataframe into new, middle, old based on age grouping"""
    new = df[df[age_col] <= 7].reset_index(inplace=True, drop=True)
    middle = df[df[age_col] > 7 and df[age_col] < 30].reset_index(inplace=True, drop=True)
    old = df[df[age_col] == 30].reset_index(inplace=True, drop=True)
    return new, middle, old

## look through each, have to inspect manually
## then restructure

def recombine(df_list, age_col="post_age"):
    """returns df of possible jobs after manual inspection by age"""
    df = pd.concat(df_list).sort_values(by=age_col).reset_index(drop=True)
    return df

def language_check(df, languages=["python"], regex_langs=None, desc_col="jobDescription"):
    """takes a list of languages, adds col to df for each and returns true if lang present in description"""
    for lang in languages:
        df[f"{lang}"] = df[desc_col].str.lower().str.contains(lang, regex=False)
    if regex_langs is not None:
        for lang in regex_langs:
            df[f"{lang}"] = df[desc_col].str.lower().str.contains(lang, regex=True)
    return df

def remove_viewed(possible_keys):
    with open("viewed_jobs.pkl", "rb") as f:
        viewed = pickle.load(f)
    for key in possible_keys:
        if key in viewed:
            possible_keys.remove(key)

def save_possible_keys(df, filepath=f"jobkeys{date}.pkl", key_col="jobkey"):
    """saves jobkeys as pkl for use in openjobs.py after removing unwanted rows"""
    possible_keys = list(df[key_col])
    remove_viewed(possible_keys)
    with open(filepath, "wb") as file:
        pickle.dump(possible_keys, file)

if __name__ == "__main__":
    df = pd.read_json(file)
    df = clean_indeed_data(df, ["salarymin", "salarymax"], "salarytype", "post_age", "jobDescription", True, 4, False)
    df.to_pickle(f"jobs{date}.pkl")