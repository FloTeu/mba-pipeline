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
from .html_handler import HtmlHandler 
from plotly.offline import plot
import plotly.graph_objs as go
from plotly.graph_objs import Scatter
from django.core.paginator import Paginator

register = template.Library()

#def homepage(request):
    #return HttpResponse("Ich geh dir fremd :O")
#    return render(request, "main.html")

def about(request):
    return render(request, 'about.html')
    
def impressum(request):
    return render(request, 'impressum.html')

def contact(request):
    return render(request, 'contact.html')

def dataprotection(request):
    return render(request, 'dataprotection.html')

def main(request):
    iterator=itertools.count()
    marketplace = "de"
    
    DataHandlerModel = DataHandler()

    sort_by = request.GET.get('sort_by')
    bsr_min = request.GET.get('bsr_min')
    bsr_max = request.GET.get('bsr_max')
    desc = request.GET.get('direction')
    info = request.GET.get('info')
    filter = request.GET.get('filter')
    columns = request.GET.get('columns')
    rows = request.GET.get('rows')
    key = request.GET.get('s')
    page = request.GET.get('page')

    if filter == "0":
        filter = "only 0"
    elif filter == "404":
        filter = "only 404"
    #q_desc = request.GET["direction"]

    df_shirts = DataHandlerModel.get_shirts(marketplace, limit=None, in_test_mode=True, filter=filter)
    df_shirts = df_shirts.round(2)
    dict_min_max = {"dict_min_max": DataHandlerModel.get_min_max_dict(df_shirts)}

    if key != None:
        df_shirts = df_shirts.dropna()
        df_shirts = df_shirts[df_shirts.apply(lambda x: key.lower() in x.product_features.lower() or key.lower() in x.title.lower() or key.lower() in x.asin.lower(), axis=1)]
        #df_shirts  = df_shirts[df_shirts["product_features"].str.contains(key, case=False)]

    if sort_by != None:
        if desc == "desc":
            if True: 
                df_shirts = df_shirts[(df_shirts["bsr_max"]!=0) & (df_shirts["bsr_last"]!=404) & (~df_shirts["upload_date"].isnull())].sort_values(sort_by, ascending=False)
            else:
                df_shirts = df_shirts.sort_values(sort_by, ascending=False)
        else:
            if True: 
                df_shirts = df_shirts[(df_shirts["bsr_max"]!=0) & (df_shirts["bsr_last"]!=404) & (~df_shirts["upload_date"].isnull())].sort_values(sort_by, ascending=True)
            else:
                df_shirts = df_shirts.sort_values(sort_by, ascending=True)

    number_shirts = len(df_shirts)
    if columns == None:
        columns = 6
    else:
        columns = int(columns)
    if (number_shirts / columns) < 1:
        row_max = 1
        columns = int(columns * (number_shirts / columns))
    else:
        row_max = int(number_shirts / columns)

    if rows == None:
        rows = 10
    else:
        rows = int(rows)
    if rows > row_max:
        rows = row_max
        
    if page == None:
        page = 1
    else:
        page = int(page)


    # filter dataframe by given min max 
    if bsr_min != "" and bsr_min != None and bsr_max != "" and bsr_max != None and sort_by != "" and sort_by != None:
        df_shirts = df_shirts.loc[(df_shirts["bsr_last"] >= float(bsr_min)) & (df_shirts["bsr_last"] <= float(bsr_max))]

    # pagination
    asin_list = df_shirts["asin"].tolist()[0:len(df_shirts["asin"].tolist())]
    if len(asin_list) == 0:
        paginator = Paginator(["Empty"], 1)
    else:
        paginator = Paginator(asin_list, (columns*rows))
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    if len(df_shirts) > 0:
        df_shirts = df_shirts.iloc[(page-1)*(columns*rows):((page-1)*(columns*rows) + (columns*rows))]
        #df_shirts["plot"] = df_shirts.apply(lambda x: DataHandlerModel.create_plot_html(x, df_shirts_detail_daily), axis=1)
        df_shirts_plots = DataHandlerModel.get_df_plots("de", df_shirts["asin"].tolist())
        df_shirts = df_shirts.join(df_shirts_plots.set_index('asin'), on='asin')

    shirt_info = df_shirts.to_dict(orient='list')
    print(len(df_shirts))
    print(page, (page-1)*(columns*rows), ((page-1)*(columns*rows) + (columns*rows)))

    #context = {"asin": ["awdwa","awdwawdd", "2312313"],}
    output_dict = {"shirt_info":shirt_info,'page_obj': page_obj, "iterator":iterator, "columns" : columns, "rows": rows,"show_detail_info":info}
    request_dict = dict(request.GET)
    if key != None and key == "":
        del request_dict["s"]
    output_dict.update(request_dict)

    HtmlHandlerModel = HtmlHandler(df_shirts)
    shirts_html = HtmlHandlerModel.create_shirts_html()
    output_dict.update({"shirts_html": shirts_html})
    output_dict.update(dict_min_max)

    return render(request, 'main.html', output_dict)
    #return HttpResponse(template.render(context, request))

#df_shirts = get_shirts("de", limit=None, in_test_mode=False)
#df_shirts.to_csv("mba-pipeline/mba-page/merchwatch/merchwatch/data/shirts2.csv", index=None, sep="\t")
#test = 0
