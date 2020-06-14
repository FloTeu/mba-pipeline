from django.http import HttpResponse
from django.shortcuts import render 
from django.template import loader
from django import template
import pandas as pd
from google.cloud import bigquery
import itertools

register = template.Library()

#def homepage(request):
    #return HttpResponse("Ich geh dir fremd :O")
#    return render(request, "main.html")

def about(request):
    return HttpResponse("about")


def get_sql(marketplace, limit):
    if limit == None:
        SQL_LIMIT = ""
    elif type(limit) == int and limit > 0:
        SQL_LIMIT = "LIMIT " + str(limit)
    else:
        assert False, "limit is not correctly set"

    
    SQL_STATEMENT = """
    SELECT t0.*, t1.url, Date(t2.upload_date) as upload_date FROM (
    SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
            AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min,
            AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
            FROM `mba-pipeline.mba_{}.products_details_daily`
    where bsr != 0 and bsr != 404
    group by asin
    ) t0
    left join `mba-pipeline.mba_de.products_images` t1 on t0.asin = t1.asin
    left join `mba-pipeline.mba_de.products_details` t2 on t0.asin = t2.asin
    order by t0.bsr_mean
    {}
    """.format(marketplace, SQL_LIMIT)
    return SQL_STATEMENT

def get_shirts(marketplace, limit=None, in_test_mode=False):
    import os 
    print(os.getcwd())
    if in_test_mode:
        df_shirts=pd.read_csv("merchiewer/data/shirts.csv")
    else:
        project_id = 'mba-pipeline'
        bq_client = bigquery.Client(project=project_id)
        df_shirts = bq_client.query(get_sql(marketplace, limit)).to_dataframe().drop_duplicates()

    return df_shirts
    
def main(request, filter=None):
    iterator=itertools.count()
    latest_question_list = [{"name":"Florian"},{"name":"Chiara"},{"name":"Simone"}]
    marketplace = "de"
    df_shirts = get_shirts(marketplace, limit=30, in_test_mode=True).head(100)
    df_shirts = df_shirts.sort_values(filter, ascending=False)
    shirt_info = df_shirts.to_dict(orient='list')
    #context = {"asin": ["awdwa","awdwawdd", "2312313"],}

    return render(request, 'main.html', {"shirt_info":shirt_info, "iterator":iterator, "columns" : 6, "rows": 2, "filter":filter})
    #return HttpResponse(template.render(context, request))

#df_shirts = get_shirts("de", limit=None, in_test_mode=True)
#df_shirts.to_csv("mba-pipeline/mba-page/merchiewer/merchiewer/data/shirts.csv", index=None)
#test = 0