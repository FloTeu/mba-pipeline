from django.http import HttpResponse
from django.shortcuts import render 
from django.template import loader
from django import template
import pandas as pd
from google.cloud import bigquery

register = template.Library()

#def homepage(request):
    #return HttpResponse("Ich geh dir fremd :O")
#    return render(request, "main.html")

def about(request):
    return HttpResponse("about")


def get_sql(marketplace):
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
    LIMIT 30
    """.format(marketplace)
    return SQL_STATEMENT

def get_shirts(marketplace):
    project_id = 'mba-pipeline'
    bq_client = bigquery.Client(project=project_id)
    df_shirts = bq_client.query(get_sql(marketplace)).to_dataframe().drop_duplicates()

    return df_shirts

from django import template
register = template.Library()

@register.filter
def index(indexable, i):
    return indexable[i]
    
def main(request):
    latest_question_list = [{"name":"Florian"},{"name":"Chiara"},{"name":"Simone"}]
    marketplace = "de"
    df_shirts = get_shirts(marketplace)
    context = df_shirts.to_dict(orient='list')
    #context = {"asin": ["awdwa","awdwawdd", "2312313"],}

    return render(request, 'main.html', context)
    #return HttpResponse(template.render(context, request))

#df_shirts = get_shirts("de")
#test = 0