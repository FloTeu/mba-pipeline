from bs4 import BeautifulSoup
import requests 
import pandas as pd

# This script is currently only implemented for Hobbies list of wikipedia 
bq_table_id = 'keywords.en_hobbies'

url = 'https://en.wikipedia.org/wiki/List_of_hobbies'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
wiki_body_div = soup.find_all("div", class_="mw-parser-output")[0]

df_keyword_hobbies = pd.DataFrame(columns=["ID","category_main", "category_specification", "hobby"])

# iterate over all tags (Children) within th body div
# Start with index 8, because between index 0 and 8 only unnecessary indrocution Text can be found.
category_main = ""
category_specification = ""
count_inserts = 0
for wiki_content in list(wiki_body_div.children)[8:-1]:
    if wiki_content == '\n':
        continue
    if wiki_content.name == "h2":
        category_main = wiki_content.text.replace("[edit]", "")
    if wiki_content.name == "h3":
        category_specification = wiki_content.text.replace("[edit]", "")
    # if div is found we can crawl all listed hobbies
    if wiki_content.name == "div" and "reflist" not in list(wiki_body_div.children)[8:-1][4].attrs["class"]:
        # iterate over list of hobbies
        for i, hobby in enumerate(wiki_content.find_all("li")):
            try:
                hobby_name = list(hobby.children)[0].getText()
            except:
                try:
                    hobby_name = hobby.getText()
                except:
                    # if no hobby name can be extracted, the loop should continue
                    conitnue
            df_keyword_hobbies.loc[count_inserts] = [count_inserts + 1, category_main, category_specification, hobby_name]
            count_inserts += 1

df_keyword_hobbies.to_gbq(bq_table_id,project_id="mba-pipeline", if_exists="fail")
test = 0

