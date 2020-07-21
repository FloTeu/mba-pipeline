from django.http import HttpResponse
from django.shortcuts import render 
from django.template import loader
from django import template
import pandas as pd
from google.cloud import bigquery
import itertools
from sklearn import preprocessing
import os 
from datetime import date
import datetime 
import time 
from .data_handler import DataHandler 


register = template.Library()

#def homepage(request):
    #return HttpResponse("Ich geh dir fremd :O")
#    return render(request, "main.html")

def about(request):
    return HttpResponse("about")
    
def main(request):
    iterator=itertools.count()
    marketplace = "de"
    
    DataHandlerModel = DataHandler()

    sort_by = request.GET.get('sort_by')
    desc = request.GET.get('direction')
    info = request.GET.get('info')
    filter = request.GET.get('filter')
    columns = request.GET.get('columns')
    rows = request.GET.get('rows')
    key = request.GET.get('s')

    if filter == "0":
        filter = "only 0"
    elif filter == "404":
        filter = "only 404"
    #q_desc = request.GET["direction"]

    df_shirts, df_shirts_detail_daily = DataHandlerModel.get_shirts(marketplace, limit=None, in_test_mode=True, filter=filter)
    df_shirts = df_shirts.round(2)

    if key != None:
        df_shirts = df_shirts[df_shirts.apply(lambda x: key.lower() in x.product_features.lower() or key.lower() in x.title.lower(), axis=1)]
        #df_shirts  = df_shirts[df_shirts["product_features"].str.contains(key, case=False)]

    if sort_by != None:
        if desc == "desc":
            if "bsr" in sort_by or "trend" in sort_by: 
                df_shirts = df_shirts[(df_shirts["bsr_max"]!=0) & (df_shirts["bsr_last"]!=404)].sort_values(sort_by, ascending=False)
            else:
                df_shirts = df_shirts.sort_values(sort_by, ascending=False)
        else:
            if "bsr" in sort_by or "trend" in sort_by: 
                df_shirts = df_shirts[(df_shirts["bsr_max"]!=0) & (df_shirts["bsr_last"]!=404)].sort_values(sort_by, ascending=True)
            else:
                df_shirts = df_shirts.sort_values(sort_by, ascending=True)

    number_shirts = len(df_shirts)
    if columns == None:
        columns = 6
    else:
        columns = int(columns)
    row_max = int(number_shirts / columns)

    if rows == None:
        rows = 5
    else:
        rows = int(rows)
    if rows > row_max:
        rows = row_max
        
    shirt_info = df_shirts.to_dict(orient='list')
    #context = {"asin": ["awdwa","awdwawdd", "2312313"],}
    return render(request, 'main.html', {"shirt_info":shirt_info, "iterator":iterator, "columns" : columns, "rows": rows,"show_detail_info":info, "sort_by":sort_by})
    #return HttpResponse(template.render(context, request))

#df_shirts = get_shirts("de", limit=None, in_test_mode=False)
#df_shirts.to_csv("mba-pipeline/mba-page/merchwatch/merchwatch/data/shirts2.csv", index=None, sep="\t")
#test = 0
