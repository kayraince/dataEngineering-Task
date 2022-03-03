import base64
import hashlib
import hmac
import json
import requests
import pandas as pd
simply_hired = pd.read_csv('csv/simplyhired_scrape.csv')
url_dict = dict()
file = open('csv/theladders_api_links.csv')
for line in file:
    line = line.strip('\n')
    (key, val) = line.split(",")
    url_dict[key] = val
apiList = list(url_dict.values())
dfList = []
statelist = []


def fetch(url):
    request = requests.get(url)
    response_main = (json.loads(request.content))
    response = response_main.get('job')
    df = pd.DataFrame.from_dict(response, orient="index")
    df_transpose = df.transpose()
    dfList.append(df_transpose)
    try:
        state = response.get('location')
        item = state.strip("\n")
        n_item = item.split(",")
        statelist.append(n_item[1])
    except IndexError:
        statelist.append("n/a")


    print('fetching...' + url)
    return 200


def generate_data():
    final = pd.concat(dfList)
    final_df = pd.DataFrame.drop(final, columns=['company', 'locations', 'classifications', 'lowerBand', 'upperBand', 'isLaddersEstimate', 'compensationBonus', 'compensationOther', 'jobId', 'otherLocations', 'yearsExperience', 'postingDate', 'recruiterAnonymous', 'companyIsConfidential', 'industryName', 'industryId', 'score', 'promoted', 'jobLocationId', 'coordinates', 'marketing', 'active', 'allowExternalApply', 'numberOfApplications', 'jobStatus', 'entryDate', 'salaryIsConfidential', 'questions',  'minMonthsExperience', 'featured', 'currentlyFeatured', 'promotedLabelVisible', 'recruiterId', 'recruiterFirstName', 'recruiterLastName', 'encodedRecruiterId'])
    final_rn_df = pd.DataFrame.rename(final_df, columns={'title': 'job_title', 'fullDescription': 'job_description', 'companyName': 'company', 'seoJobLink': 'job_page_url'})
    final_rn_df['state'] = statelist
    final_df_reindex = final_rn_df.reindex(columns=['company', 'job_description', 'job_page_url', 'job_title', 'location', 'state', 'zipcode'])
    return final_df_reindex


def csv_upload():
    simply_hired = pd.read_csv('csv/simplyhired_scrape.csv')
    propellum = pd.read_csv('csv/propellum_scrape.csv')
    indeed = pd.read_csv('csv/indeed_scrape.csv')
    columnList = []
    df_indeed = pd.DataFrame.drop(indeed, columns=['qualifications', 'scrape_time', 'benefits', 'jk', 'job_date', 'job_details', 'job_label', 'rating', 'salary'])
    df_simply_hired = pd.DataFrame.drop(simply_hired, columns=['qualifications', 'scrape_time', 'benefits', 'jk', 'job_date', 'job_details', 'job_label', 'rating', 'salary'])
    df_propellum = pd.DataFrame.drop(propellum, columns=['Qualification', 'SOC Code', 'Job Id', 'Status', 'Job Opening Date', 'Job Closing Date'])

    df_rn_propellum = pd.DataFrame.rename(df_propellum, columns= {'Job Title': 'job_title', 'Job Description': 'job_description', 'Company Name': 'company', "Qualification": 'qualifications', 'City': 'location', 'State': 'state', 'Zipcode': 'zipcode', 'Website Url': 'job_page_url'})
    df_fnl_propellum = df_rn_propellum.reindex(columns=['company', 'job_description', 'job_page_url', 'job_title', 'location', 'state', 'zipcode'])

    columnList.append(df_fnl_propellum)
    columnList.append(df_indeed)
    columnList.append(df_simply_hired)
    merged_df = pd.concat(columnList)
    return merged_df


for i in apiList[1:95]:
    fetch(i)


frame_build_in = generate_data()
frame_fetched = csv_upload()
frames = [frame_fetched, frame_build_in]
final_product = pd.concat(frames)
print(final_product)
final_product.to_csv('/Users/kayra/Desktop/grey.csv')
