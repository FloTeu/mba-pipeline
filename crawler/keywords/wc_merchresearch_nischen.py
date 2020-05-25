from bs4 import BeautifulSoup
import requests
import pandas as pd
# import argparse
import sys

# This script is currently only implemented for Hobbies list of wikipedia
def main(argv):
    # parser = argparse.ArgumentParser(description='')
    # # Decide wheter to choose englisch or german wikipedia page of hobbies
    # parser.add_argument('language', help='Language of wikipedia page. I.e "en" or "de"', type=str)
    #
    # # get all arguments
    # args = parser.parse_args()
    # page_language = args.language


    #bq_table_id = 'keywords.' + page_language + '_nischen'
    bq_table_id = 'keywords.' + 'de' + '_nischen'
    project_id = "mba-pipeline"

    # if page_language == "de":
    #     url = 'https://de.qwe.wiki/wiki/List_of_hobbies'
    # else:
    #     url = 'https://en.wikipedia.org/wiki/List_of_hobbies'
    url = 'https://merchreport.de/mba-nischenliste/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    merchreport_table_div = soup.find_all("tbody") # , class_="et_pb_row")[0]

    # df_keyword_hobbies = pd.DataFrame(columns=["ID","category_main", "category_specification", "hobby"])
    df_keyword = pd.DataFrame(columns=["ID", "category_main", "nische"])

    # iterate over all tags (Children) within th body div
    # Start with index 8, because between index 0 and 8 only unnecessary indrocution Text can be found.
    category_main = ""
    category_specification = ""
    count_inserts = 0
    for tbody in merchreport_table_div: #[8:-1]:
        category_main = tbody.find("h3").getText()
        tds = tbody.find_all("td")[1:]  # skip first as its the category
        for i, tag in enumerate(tds):
            try:
                nische = list(tag.children)[0].getText().replace("\n", "")
            except:
                try:
                    nische = tag.getText().replace("\n", "")
                except:
                    # if no hobby name can be extracted, the loop should continue
                    continue
            df_keyword.loc[count_inserts] = [count_inserts + 1, category_main, nische]
            count_inserts += 1

    df_keyword.to_gbq(bq_table_id,project_id=project_id, if_exists="replace")

if __name__ == '__main__':
    main(sys.argv)

