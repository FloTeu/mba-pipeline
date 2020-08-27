import pandas as pd 
import argparse
import sys 


def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--asin', default="", help='ASIN of mba product', type=str)
    parser.add_argument('--url_affiliate', default="", help='Affiliate url', type=str)
    parser.add_argument('--csv_path', default="", help='Path to csv with asin and url_affiliate', type=str)


    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    asin = args.asin
    url_affiliate = args.url_affiliate
    csv_path = args.csv_path

    dest_table = "mba_" + marketplace + ".products_affiliate_urls"
    project_id = "mba-pipeline"

    if asin != "" and url_affiliate != "":
        df_affiliate = pd.DataFrame(data={"asin": [asin], "url_affiliate": [url_affiliate]})
        df_affiliate.to_gbq(dest_table, project_id=project_id, if_exists="append")
    if csv_path != "":
        df_affiliate = pd.read_csv(csv_path)[["asin", "url_affiliate"]]
        df_affiliate.to_gbq(dest_table, project_id=project_id, if_exists="append")
    
if __name__ == '__main__':
    main(sys.argv)