import pandas as pd
import json

from classes import MSTR


def main():
    mstr = MSTR("MSTR_CONN")

    authToken, cookies = mstr.login()

    header = mstr.set_base_headers(authToken=authToken)

    result = mstr.json_to_dataframe(mstr.get_cube_information(header=header,
                                                              cookies=cookies,
                                                              cube_id="FC21A94245FBC13FE1A5A2BEB6288F7E"))
    df = mstr.folder_iteration(header=header, cookies=cookies, root="68BFCE134EB6CB3EFAC082AB18B8A5FD")

    df_cubes = df[df["subtype"] == 776]
    cl = df_cubes["id"].to_list()

    temp_store = []

    for c in cl:
        r = mstr.get_cube_information(header=header,
                                      cookies=cookies,
                                      cube_id=c)

        tr = mstr.json_to_dataframe(r)

        temp_store.append(tr)

    final = pd.concat(temp_store)

    final.to_csv("final.csv")

    # print(final)

    print(f'Received AuthenticationToken: %s \nCookies: %s' % (authToken,
                                                               cookies))
    return final


df = main()

json_struc = json.loads(df.to_json(orient="records"))
flattened = pd.json_normalize(json_struc)
flattened.rename(columns={flattened.columns[2]: "list_of_metrics",
                          flattened.columns[3]: "list_of_attributes"})

print(df)
