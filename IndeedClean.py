import numpy as np
import pandas as pd
import re
#look through description for salaries
def update_salary_from_description(df, salary_cols, desc_col, type_col=None):
    """
    Look through description col to find salary if salary is null
    
    Parameters
    ----------
    df: DataFrame
    salary_cols: list
        list of string names of cols containing salary values
    desc_col: str
        col name containing job description
    type_col: str
        default None, str col name containing yearly/hourly info
        
    Returns
    -------
    DataFrame
        updated salaries from description, updated salary type if type_col specified
    """
    #Works inplace
    exp = r"\$(\d+,*\.*\d*)[^A-Za-z]"
    scols = df[salary_cols]
    if type_col is not None:
        types = df[type_col]
        change = True
    #loop through df, if salary null look at description
    for i in range(df.shape[0]):
        if not df.iloc[i].isna()[salary_cols[0]]:
            continue
        sal = re.findall(exp, df.iloc[i][desc_col])
        if len(sal) == 0:
            continue
        elif len(sal) == 1:
            sal.append(sal[0])
        #clean sal strings
        sal = list(map(lambda x: np.float64(x.replace(",","")), sal))
        if len(sal) > 2:
            sal = [np.min(sal), np.max(sal)]
        #spotcheck sal for validity
        #too low, probably wrong
        if sal[0] < 15 or sal[1] < 15:
            continue
        #doesn't fit into hourly or yearly averages
        elif (sal[0] > 100 and sal[0] < 50000) or (sal[1] > 100 and sal[1] < 50000):
            continue
        #likely one given hourly and one given yearly
        elif sal[0] < 100 and (sal[1] >= 1000 and sal[1] <= 10000):
            sal[1] = sal[0]
        #same as above
        elif sal[0] < 100 and sal[1] >= 50000:
            sal[0] = sal[1]
        #inconsistent salaries
        elif sal[0] > 60 and sal[1] <= 10000:
            continue
        scols.iloc[i] = sal
        if change:
            if sal[0] >= 10000:
                types.iloc[i] = "yearly"
            else:
                types.iloc[i] = "hourly"
    if change:
        df[type_col] = types
    df[salary_cols] = scols
    return df


#clean salary cols
def convert_yearly_to_hourly(df, cols_to_change, type_col):
    """
    Convert yearly salary to hourly assuming 52 weeks at 40 hours/wk and update the salarytype
    
    Parameters
    -----------
    df: DataFrame
    cols_to_change: ["col1", "col2"]
        list of column names to modify in string format
    type_col: str 
        column name seperating 'yearly' and 'hourly' types
        
    Returns
    -------
    DataFrame
        specified cols now in hourly salary, all entries in type_col now 'hourly'
    """
    df.sort_values(by=type_col)
    yearly, hourly = df[df[type_col] == "yearly"], df[df[type_col] == "hourly"]
    null = df[df[type_col].isna()]
    for col in cols_to_change:
        yearly[col] = np.round(yearly[col]/(42*50), 2)
    yearly[type_col] = yearly[type_col].str.replace("yearly", "hourly")
    null[type_col] = null[type_col].fillna("hourly")
    df = pd.concat([hourly, null, yearly]).sort_index()
    return df


def fill_null_salary(df, salary_cols, value=None):
    """Fills salary columns null values
    
    Parameters
    ----------
    df: DataFrame
    salary_cols: list
        contains labels of columns holding the salary data
    value: list or numeric
        default None, calculates and uses the average per column to fill na, otherwise uses value
        
    Returns
    --------
    DataFrame
        null entries in salary replaced
    """
    if value is not None and type(value) is list:
        assert len(value) == len(salary_cols)
        for i, col in enumerate(salary_cols):
            df[col].fillna(value[i], inplace=True)
    elif value is not None and type(value) is not list:
        df[salary_cols].fillna(value)
    else: 
        for col in salary_cols:
            value = np.round(df[col].mean(), 2)
            df[col] = df[col].fillna(value)
    return df


#clean post age to integers
def clean_post_age(df, age_col):
    """Turn post age into integers
    """
    df[age_col] = df[age_col].str.replace("Today", "0").str.replace("Just posted", "0").str.strip("+ days ago").astype(int)
    return df


#use above functions to clean data
def clean_indeed_data(df, salary_cols, type_col, age_col, desc_col, drop_meta=False, n_meta=1,
                     drop_desc=False):
    """
    Updates salary, salary types, age of post, and fills na. Can drop meta and description cols
    
    Parameters
    ----------
    df: DataFrame
        dataframe containing indeed data to be cleaned
    salary_cols: list
        list of column names (as strings) storing salary info in df
    type_col: str
        label of the column containing yearly or hourly salary description
    desc_col: str
        label of the column containing the job description
    drop_meta: bool
        default False, if True drops meta columns (keyword, location, etc...). If True, the meta columns must be the first n columns
    n_meta: int
        default 1, number of columns to drop if True passed to drop_meta
    drop_desc: bool
        default False, if True drops the job description column. Not recommended if further analysis is desired from the job description.
        
    Returns
    -------
    DataFrame
        details of cleaned df can be found in individual functions:
        
    See Also
    --------
    update_salary_from_description: function updates salary cols
    convert_yearly_to_hourly: function consolidates salary to hourly
    fill_null_salary: function fills empty salaries with average of non null
    clean_post_age: function converts post age to int
    """
    if drop_meta:
        to_drop = list(df.columns[:n_meta])
        df.drop(labels=to_drop, axis=1, inplace=True)
    if drop_desc:
        df.drop(labels=desc_col, axis=1, inplace=True)
    df = update_salary_from_description(df, salary_cols=salary_cols,
                                        desc_col=desc_col, type_col=type_col)
    df = convert_yearly_to_hourly(df, cols_to_change=salary_cols, type_col=type_col)
    df = fill_null_salary(df, salary_cols)
    df = clean_post_age(df, age_col)
    return df

def combine_results(df1, df2):
    """Combine dataframes on top of each other. Used after clean_indeed_data() for multiple search results.
    """
    assert np.all(df1.columns == df2.columns)
    df = pd.concat([df1, df2])
    return df