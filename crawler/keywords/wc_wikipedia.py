from bs4 import BeautifulSoup
import requests 
import pandas

url = 'https://en.wikipedia.org/wiki/List_of_hobbies'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
wiki_body_div = soup.find_all("div", class_="mw-parser-output")[0]

df_keyword_hobbies = pd.DataFrame(columns=["category_main", "category_specification", "hobby"])

# iterate over all tags (Children) within th body div
# Start with index 8, because between index 0 and 8 only unnecessary indrocution Text can be found.
category_main = ""
category_specification = ""
for wiki_content in list(wiki_body_div.children)[8:-1]:
    if wiki_content == '\n':
        continue
    if wiki_content.name = "h2":
        category_main = wiki_content.text.replace("[edit]", "")



test = 0

