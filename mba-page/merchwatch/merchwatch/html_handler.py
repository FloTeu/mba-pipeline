


class HtmlHandler():
    def __init__(self, df_shirts):
        self.df_shirts = df_shirts

    def get_shirt_additional_info(self, df_shirt):
        html = """<p style="margin:0">price_mean: {{ shirt_info.price_mean|index:counter_total }}</p>
                                    <p style="margin:0">price_max: {{ shirt_info.price_max|index:counter_total }}</p>
                                    <p style="margin:0">price_min: {{ shirt_info.price_min|index:counter_total }}</p>
                                    <p style="margin:0">bsr_mean: {{ shirt_info.bsr_mean|index:counter_total }}</p>
                                    <p style="margin:0">bsr_max: {{ shirt_info.bsr_max|index:counter_total }}</p>
                                    <p style="margin:0">bsr_min: {{ shirt_info.bsr_min|index:counter_total }}</p>
                                    <p style="margin:0">bsr_last: {{ shirt_info.bsr_last|index:counter_total }}</p>
                                    <p style="margin:0">bsr_first: {{ shirt_info.bsr_first|index:counter_total }}</p>
                                    <p style="margin:0">bsr_count: {{ shirt_info.bsr_count|index:counter_total }}</p>
                                    <p style="margin:0">trend: {{ shirt_info.trend|index:counter_total }}</p>
                                    <p style="margin:0">score_mean: {{ shirt_info.score_mean|index:counter_total }}</p>
                                    <p style="margin:0">score_max: {{ shirt_info.score_max|index:counter_total }}</p>
                                    <p style="margin:0">upload_date: {{ shirt_info.upload_date|index:counter_total }}</p>"""
        return html
    def get_shirt_html(self, df_shirt):
        detail_info = ""
        plot_html = df_shirt["plot"]
        text_color_trend_change = "text-black"
        if int(df_shirt["trend_change"]) > 0:
            text_color_trend_change = "text-green-600"
        if int(df_shirt["trend_change"]) < 0:
            text_color_trend_change = "text-red-600"

        html = """
                            <div class="w-1/6 p-2">
                                <div class="text-gray-700 text-center bg-gray-400 p-2">
                                    <div class="tooltip">Titel anzeigen
                                        
                                    
                                    <span class="tooltiptext"><p style="margin:0">Titel: {0} </p> </span></div>
                                    <p style="margin:0">asin: {1}</p>
                                    {2}
                                    <div class = "">
                                    <div class="md:flex-shrink container">
                                        <a href={3} target="_blank">
                                            <img class="rounded-lg w-full" src={4} alt="Shirt could not be loaded" width=200 height=200>
                                            <div class="bottom-left p-1 text-xs rounded-lg bg-white text-black">Trend: {5}</div>
                                            <div class="bottom-right p-1 text-xs rounded-lg bg-white {8}">Change: {6}</div>
                                        </a>   
                                        </div> 
                                        
                                    <div class="h-32"> 
                                      {7}
                                    </div>    
                                </div>
                                </div>
                                </div>
                """.format(df_shirt["title"], df_shirt["asin"],detail_info, "http://www.amazon.de/dp/" + df_shirt["asin"], df_shirt["url"],df_shirt["trend_nr"],df_shirt["trend_change"],plot_html, text_color_trend_change)
        return html
        
    def create_shirts_html(self):
        html = ""
        for i, shirt in self.df_shirts.iterrows():
            html = html + self.get_shirt_html(shirt)
        return html
        

