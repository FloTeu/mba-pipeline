from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

def list_str_to_list(list_str):
    list_str = list_str.strip("[]")
    split_indices = []
    index_open = True
    quote_type = ""
    split_index = []
    for i, char in enumerate(list_str):
        if char == "'" or char == '"':
            if index_open:
                quote_type = char
                split_index.append(i)
                index_open = False
            # if we want a closing index two conditions must be fit 1. closing char must be equal to opening char + (comma char in next 3 chars or string ends in the next 3 chars)
            elif ("," in list_str[i:i+4] or i+4 > len(list_str)) and char == quote_type: 
                split_index.append(i)
                split_indices.append(split_index)
                # reset split index
                split_index = []
                # search for next index to open list element
                index_open = True
    list_return = []
    for split_index in split_indices:
        list_element = list_str[split_index[0]:split_index[1]]
        list_return.append(list_element)
    return list_return

class MerchwatchShirt():
    def __init__(self, marketplace, platform="mba", language="de", title="", brand="", listings="", description="", tags="", asin=""):
        self.marketplace = marketplace
        self.platform = platform
        self.language = language
        self.title = title
        self.brand = brand
        self.listings = listings
        self.description = description
        self.tags = tags
        self.asin = asin
        self.description = description
        self.keywords = ""

        self.keywords_to_remove_de = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "Geschenk", "Geschenke", "Geschenkidee", "Design", "Weihnachten", "Frau",
        "Geburtstag", "Freunde", "Sohn", "Tochter", "Vater", "Geburtstagsgeschenk", "Herren", "Frauen", "Mutter", "Schwester", "Bruder", "Kinder", 
        "Spruch", "Fans", "Party", "Geburtstagsparty", "Familie", "Opa", "Oma", "Liebhaber", "Freundin", "Freund", "Jungen", "Mädchen", "Outfit",
        "Motiv", "Damen", "Mann", "Papa", "Mama", "Onkel", "Tante", "Nichte", "Neffe", "Jungs", "gift", "Marke", "Kind", "Anlass", "Jubiläum"
        , "Überraschung", "Männer", "Enkel", "Enkelin", "Deine", "Deiner", "Irgendwas", "Irgendetwas", "Idee"]
        self.keywords_to_remove_en = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "gift", "Brand", "family", "children", "friends", "sister", "brother",
         "childreen", "present", "boys", "girls", "women", "woman", "men"]
        self.keywords_to_remove_dict = {"de": self.keywords_to_remove_de, "com": self.keywords_to_remove_en}
        self.keywords_to_remove = self.keywords_to_remove_de + self.keywords_to_remove_en + stopwords.words('german') + stopwords.words('english')
        self.keywords_to_remove_lower = [v.lower() for v in self.keywords_to_remove]


        # keywords are not stemped related to the language of the text but of the marketplace they are uploaded to
        # Background: user searches for data in marketplace and might write english keyword but want german designs
        # Use Case: User searches for "home office". keyword text is german but includes keyword "home office".
        # Solution: stem dependend on marketplace not language of keyword text
        if self.marketplace == "de":
            self.stop_words = set(stopwords.words('german'))  
            self.stemmer = SnowballStemmer("german")
        else:
            self.stop_words = set(stopwords.words('english'))
            self.stemmer = SnowballStemmer("english")

    def is_product_feature_listing(self, product_feature):
        """If on bullet point/ product feature is a listing created by user (contains relevant keywords)"""
        if self.marketplace == "com":
            if any(indicator in product_feature.lower() for indicator in ["solid color", "imported", "machine wash cold", "lightweight", "classic fit"]):
                return False
            else:
                return True
        else:
            if any(indicator in product_feature.lower() for indicator in ["unifarben", "pflegehinweis", "klassisch geschnitten", "polyester", "grau meliert"]):
                return False
            else:
                return True

    def cut_product_feature_list(self, product_features_list):
        if self.marketplace == "de":
            product_features_list = [feature for feature in product_features_list if self.is_product_feature_listing(feature)]
        if self.marketplace == "com":
            product_features_list = [feature for feature in product_features_list if self.is_product_feature_listing(feature)]
        return product_features_list

    def extract_listings(self, product_features_array_str):
        product_features_list = list_str_to_list(product_features_array_str)
        product_features_list = [v.strip("'").strip('"') for v in product_features_list]
        return self.cut_product_feature_list(product_features_list)

    def load_by_dict(self, df_row_dict):
        if "title" in df_row_dict:
            self.title = df_row_dict["title"]
        if "brand" in df_row_dict:
            self.brand = df_row_dict["brand"]
        if "tags" in df_row_dict:
            self.tags = df_row_dict["tags"]
        if "asin" in df_row_dict:
            self.asin = df_row_dict["asin"]
        if "language" in df_row_dict:
            self.language = df_row_dict["language"]
        if "product_features" in df_row_dict:
            self.listings = self.extract_listings(df_row_dict["product_features"])

    def filter_keywords(self, keywords, single_words_to_filter=["t","du"]):
        keywords_filtered = []
        for keyword_in_text in keywords:
            if keyword_in_text[len(keyword_in_text)-2:len(keyword_in_text)] in [" t", " T"]:
                keyword_in_text = keyword_in_text[0:len(keyword_in_text)-2]
            filter_keyword = False
            if len(keyword_in_text) < 3:
                filter_keyword = True
            else:
                #keyword_to_remove_stemmed = self.stem_keyword(keyword_to_remove)
                #keyword_in_text_stemmed = self.stem_keyword(keyword_in_text)
                if keyword_in_text.lower() in self.keywords_to_remove_lower or keyword_in_text.lower() in single_words_to_filter:
                    filter_keyword = True

                # case geschenk in keyword_in_text = Geschenk T-shirt
                for keyword_to_remove_lower in self.keywords_to_remove_lower:
                    if keyword_to_remove_lower in keyword_in_text.lower().split(" "):
                        filter_keyword = True
                        break

            if not filter_keyword:
                keywords_filtered.append(keyword_in_text)
        return keywords_filtered

    def set_keywords(self, textRankModel):
        text = " ".join([self.title + "."] + [self.brand + "."] + self.listings + [self.description])
        self.keywords = textRankModel.get_unsorted_keywords(text, candidate_pos = ['NOUN', 'PROPN'], lower=False)
        self.keywords = self.filter_keywords(self.keywords)

    def get_keywords(self):
        return self.keywords

    def stem_keyword(self, keyword):
        return self.stemmer.stem(keyword)

    def set_stem_keywords(self):
        self.stem_words = []
        
        # drop keywords with more than 1 word
        keywords_single = [keyword for keyword in self.keywords if len(keyword.split(" ")) == 1]

        keywords_filtered = [w for w in keywords_single if not w in self.stop_words]
        for keyword in keywords_filtered:
            self.stem_words.append(self.stem_keyword(keyword))

    def get_stem_keywords(self):
        return self.stem_words
