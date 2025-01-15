
def matcher(df, index_line, typ, query, strategies, year=None):

    import time, requests
    
    time.sleep(0.3)
    url_match="https://affiliation-matcher.staging.dataesr.ovh/match"
    
    if year:
        year = str(year)[0:4]
        
    try:
        if year:
            r=requests.post(url_match, json={"query": query, "type": typ, "strategies": strategies, "year":year})
        else:
            r=requests.post(url_match, json={"query": query, "type": typ, "strategies": strategies})
        match = r.json()['results']
        
        now = time.strftime("%H:%M:%S")
        if match:
            print(index_line, query, match, now)
            df.at[index_line, "match"] = match
            df.at[index_line, "q"] = 'default' 
        else:
            df.at[index_line, "q"] = 'no' 

    except requests.exceptions.HTTPError as http_err:
        print(f"\n{i} -> HTTP error occurred: {http_err}")
#         df=pd.concat([df,df.iloc[index_line]], ignore_index=True)
        df.at[index_line, "q"] = 'err' 
    except requests.exceptions.RequestException as err:
        print(f"\n{i} -> Error occurred: {err}")
#         df=pd.concat([df,df.iloc[index_line]], ignore_index=True)
        df.at[index_line, "q"] = 'err' 
    except Exception as e:
        print(f"\n{i} -> An unexpected error occurred: {e}")
#         df=pd.concat([df,df.iloc[index_line]], ignore_index=True)
        df.at[index_line, "q"] = 'err' 