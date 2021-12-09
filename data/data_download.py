import requests
import argparse

pandas_ids = {
                "test_df":"1fAm2QY4dvucpLF_h9zG-xfOsJUXfVGhU",
                "train_df":"1fISYRVTmO83z1lY1LTzpBUKbeX2oUXNA",
                "val_df":"1esyONsK_FFa9K5nJo3rWNOjaVvaLaL78",
                "ens_train_df":"1f1fxlIEXCy1c_mOHgeWgiPQ_PXmrz2zV",
                "ens_val_df":"1f8FAwZJeQCgXHZ40voZ_2KxMD4JXDzrm"
            }

cvs_ids = {
                "test_df.csv":"1f_rHr_7_aW1YGr340mvrAGPehcXo4-Y_",
                "train_df.csv":"1fh0cMKhyVcoAjhXQyipM5SJJ4S8_q5a0",
                "val_df.csv":"1fJ2aCQCm9dPt0sQOH0KeX3Uh3oKrTX8P",
                "ens_train_df.csv":"1fPtVD8zrTDGpistLMuK095VqEg9zBtAb",
                "ens_val_df.csv":"1fS6C6EqrleJkpvPMMA7Ie39K1lVTILqn"
        }

dataset_dict_ids = {"dataset": "1f-JVuN-RhU8kpiDIehpJLJd9HWAcckMO"}

info_id = "1fHe5WY1b9lK30TC55FRBuD76QomDFo2V"

# Download functions from https://stackoverflow.com/questions/38511444/python-download-files-from-google-drive-using-url

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download BReATH dataset")
    parser.add_argument("-c", action='store_true', help="Download CSV files")
    parser.add_argument("-d", action='store_true', help="Download dataset dict")
    args = parser.parse_args()

    file_ids = pandas_ids
    if args.c:
        file_ids = cvs_ids
    if args.d:
        file_ids = dataset_dict_ids

    for file_name in file_ids:
        file_id = file_ids[file_name]

        print("Downloading "+file_name)
        download_file_from_google_drive(file_id, file_name)

    print("Downloading info")
    download_file_from_google_drive(info_id, "info.json")