import pandas as pd
import sqlite3


def create_single_sicode(row):
    result = None
    if row["bitCode"] == 1:
        if row["code3"] in ("AP", "AH"):
            result = "A"
        elif row["code3"] == "PRD":
            result = "B"
        elif row["code3"] == "YLD":
            result = "BpA"
    elif row["bitCode"] == 0:
        if row["code3"] in ("AP", "AH"):
            result = "H"
        elif row["code3"] == "PRD":
            result = "T"
        elif row["code3"] == "YLD":
            result = "TpH"
    return result


def add_new_columns(dataframe: pd.DataFrame):
    dataframe["bitCode"] = (dataframe["source"] == "S1")
    dataframe["siCode"] = dataframe.apply(
        lambda x: create_single_sicode(x), axis=1)

    return dataframe


def get_code2_value(code2: str):
    code2_value = code2
    if code2.find("/") != 0:
        code2_value = code2[0:"abs".find("/") - 1]
    return code2_value


def get_sources(request: dict):
    code2_value = get_code2_value(request["code2"])
    cursor.execute(f"SELECT DISTINCT source FROM db_table "
                   f"WHERE code1 LIKE '{request['code1']}%' "
                   f"AND code2 LIKE '{code2_value}%' ")
    sources = {
        (request["code1"], request["code2"]):
            [item[0] for item in cursor.fetchall()]
    }
    return sources


def get_result_data(request: dict):
    code2_value = get_code2_value(request["code2"])
    sources = get_sources(request)

    result_data = dict()
    for source in sources[(request["code1"], request["code2"])]:
        cursor.execute(f"SELECT DISTINCT updateDate, code1, code2, code3, "
                       f"bitCode, siCode FROM db_table "
                       f"WHERE code1 LIKE '{request['code1']}%' "
                       f"AND code2 LIKE '{code2_value}%' ")

        result_data[(request["code1"], request["code2"], source)] = \
            [item for item in cursor.fetchall()]

    return result_data


def get_output(request: dict):
    sources = get_sources(request)
    result_data = get_result_data(request)

    return sources, result_data


if __name__ == '__main__':
    df = add_new_columns(pd.read_csv("db_table.csv"))
    connection = sqlite3.connect("test_db")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS db_table "
                   "(date DATE, updateDate DATETIME,"
                   "code1 VARCHAR,code2 VARCHAR,code3 VARCHAR,"
                   "value DOUBLE PRECISION,source CHAR,"
                   "bitCode BOOLEAN, siCode VARCHAR NULL)")

    connection.commit()

    df.to_sql("db_table", connection, if_exists="replace", index=False)

    list_of_inputs = [
        {"code1": "shA", "code2": "W"},
        {"code1": "shA", "code2": "S"},
        {"code1": "shA", "code2": "C"},
        {"code1": "shB", "code2": "W"},
        {"code1": "shB", "code2": "S"},
        {"code1": "shB", "code2": "C"},
        {"code1": "shC", "code2": "W"},
        {"code1": "shC", "code2": "S"},
        {"code1": "shC", "code2": "C"},
    ]
    for test_input in list_of_inputs:
        output = get_output(test_input)
        print(output[0], "\n", output[1], "\n \n")

    connection.close()
