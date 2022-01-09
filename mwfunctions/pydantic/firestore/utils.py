import re


def bsr2bsr_range_value(bsr) -> int:
    # returns value between 0 and 50
    # old code: return (int(self.bsr_min/100000),math.ceil(self.bsr_max/100000))
    return int(bsr/100000)


def get_bsr_range_list(bsr_last_range: tuple, bsr_range_to_query):
    try:
        bsr_range_list = []
        if type(bsr_last_range) == tuple:
            bsr_start = bsr_last_range[0] if type(bsr_last_range[0]) == int else 0
            bsr_end = bsr_last_range[1] if type(bsr_last_range[1]) == int and bsr_last_range[1] < (bsr_start + bsr_range_to_query) else bsr_start + bsr_range_to_query # firestore filter does allow only 10 values for "IN" query
            if bsr_end > 51:
                bsr_end = 52
            # catch the case get designs without bsr
            if bsr_start > (50-bsr_range_to_query) and bsr_last_range[1] == None:
                bsr_end = bsr_end - 1
                bsr_range_list = [i for i in range(bsr_start, bsr_end)] + [99]
            else:
                bsr_range_list = [i for i in range(bsr_start, bsr_end)]
            return bsr_range_list
        else:
            return bsr_range_list
    except Exception as e:
        print(str(e))
        return []


def get_default_category_name(marketplace) -> str:
    if marketplace == "de":
        return "Bekleidung"
    else:
        return "Clothing, Shoes & Jewelry"


def get_bsr_top_category_names_list(marketplace):
    if marketplace == "de":
        return ["Fashion", "Bekleidung"]
    else:
        return ["Clothing, Shoes & Jewelry", "Kleidung, Schuhe & Schmuck"]


def get_bsr_category(array_bsr_categorie_str: str, marketplace):
    # array_bsr_categorie_str e.g. "['Spielzeug', 'Schultüten']"
    if marketplace == "de":
        try:
            bsr_category = array_bsr_categorie_str.strip("[]").split(",")[
                0].strip("'")
        except Exception as e:
            print(array_bsr_categorie_str)
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    else:
        # does not split "," which does not work for "Clothing, Shoes & Jewelry"
        try:
            bsr_category = re.findall(
                "'([^']*)'", array_bsr_categorie_str.strip("[]"))[0]
        except Exception as e:
            print(array_bsr_categorie_str)
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    if bsr_category == "404" or bsr_category == "":
        bsr_category = get_default_category_name(marketplace)
    return bsr_category