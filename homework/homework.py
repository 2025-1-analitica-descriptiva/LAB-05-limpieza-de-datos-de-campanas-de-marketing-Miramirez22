"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    import os
    import zipfile
    import pandas as pd
    from glob import glob

    input_dir = os.path.join("files", "input")
    output_dir = os.path.join("files", "output")
    os.makedirs(output_dir, exist_ok=True)

    dfs = []

    for zip_path in glob(os.path.join(input_dir, "*.zip")):
        with zipfile.ZipFile(zip_path) as z:
            for csv_name in z.namelist():
                if csv_name.endswith(".csv"):
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)

    if not dfs:
        return
    data = pd.concat(dfs, ignore_index=True)

    client = pd.DataFrame()
    client["client_id"] = data["client_id"]
    client["age"] = data["age"]
    client["job"] = data["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["marital"] = data["marital"]
    client["education"] = data["education"].str.replace(".", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA)
    client["credit_default"] = (data["credit_default"].str.lower() == "yes").astype(int)
    client["mortgage"] = (data["mortgage"].str.lower() == "yes").astype(int)
    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    campaign = pd.DataFrame()
    campaign["client_id"] = data["client_id"]
    campaign["number_contacts"] = data["number_contacts"]
    campaign["contact_duration"] = data["contact_duration"]
    campaign["previous_campaign_contacts"] = data["previous_campaign_contacts"]
    campaign["previous_outcome"] = (data["previous_outcome"].str.lower() == "success").astype(int)
    campaign["campaign_outcome"] = (data["campaign_outcome"].str.lower() == "yes").astype(int)

    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    day = data["day"].astype(str).str.zfill(2)
    month = data["month"].str.lower().map(month_map)
    campaign["last_contact_date"] = "2022-" + month + "-" + day
    campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    economics = pd.DataFrame()
    economics["client_id"] = data["client_id"]
    economics["cons_price_idx"] = data["cons_price_idx"]
    economics["euribor_three_months"] = data["euribor_three_months"]
    economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
