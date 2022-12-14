import pandas as pd
import requests as rq
import time
import datetime
import newspaper
import spacy
from spacy.language import Language
from spacy_langdetect import LanguageDetector
import sys
import os
import json
import re
import logging

current_dir = os.path.expanduser('~') + '/programmering/wire_interpreter/'
current_time = str(time.time()).split('.')[0]
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

urls_json = open(current_dir + 'urls.json')
urls = json.load(urls_json)
urls_json.close()

logging.basicConfig(filename=current_dir + 'no.log', level=logging.DEBUG,
                    format='[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s - %(message)s', force=True)
logging.getLogger()
logging.info(f'Current directory: {current_dir}')
logging.info(f'Current date: {current_date}')
logging.info(f'Current time: {current_time}')


class Articles:
    blacklisted_words = ['innkalling til ekstraordinær generalforsamling', 'nøkkelinformasjon ved innfrielse', 'Tilbakekjøp av ansvarlig obligasjonslån',
                         'offentliggjør resultat', 'inviterer til presentasjon', 'transaksjoner foretatt under tilbakekjøpsprogram', ]

    def get_lang_detector(nlp, name):
        return LanguageDetector()

    def main():
        try:
            logging.info(
                f"Trying to fetch Norwegian articles")
            site = newspaper.build(urls['no']['articles'])
            site_urls = site.article_urls()
        except:
            logging.critial(
                f"Failed fetching articles from: {urls['no']['articles']}")
            return

        logging.info(
            f'Number of articles fetched before language and blacklist check: {len(site_urls)}')

        articles = []

        try:
            logging.info(
                f'Loading Spacy NLP in Norse')
            nlp = spacy.load("en_core_web_sm")
            Language.factory('language_detector',
                             func=Articles.get_lang_detector)
            nlp.add_pipe('language_detector', last=True)
        except:
            logging.critical(
                f"Failed fetching articles from: {urls['no']['articles']}")
            return

        for article_url in site_urls:
            article = newspaper.Article(article_url)
            article.download()
            article.parse()

            if len(article.text) > 300:
                if not any(wrd in article.text[:300].lower() for wrd in Articles.blacklisted_words):
                    doc = nlp(article.text)

                    if doc._.language['language'] == 'no' and doc._.language['score'] > 0.8:
                        articles.append(article.text.lower())

        logging.info(
            f'Number of articles fetched after language and blacklist check: {len(articles)}')

        df_articles = pd.DataFrame(articles, columns=['article'])
        df_articles['date'] = current_date

        export_name = current_dir + str(current_date) + ' no_articles.csv'

        logging.debug(f'Export name: {export_name}')

        try:
            df_articles.to_csv(export_name, index=False)
            logging.info(f'Export successfull, see {export_name}')
            print(f'Export successfull, see {export_name}')
        except:
            logging.critical(f'Export of Norwegian articles failed')


class Prices:
    df_unfiltered = pd.DataFrame()
    df_fixed_keys = pd.DataFrame()
    df_filtered = pd.DataFrame()

    def populate_df(response):
        company_list = []
        pct_list = []
        ticker_list = []

        for company in response['aaData']:
            if 'XOSL' in company[4]:
                try:
                    company_list.append(company[1].split(
                        "data-title-hover='")[1].split("'>")[0].lower())
                except:
                    logging.warning(
                        f'Failed to retrieve company name for {company[1]}')

                try:
                    ticker_list.append(company[3].lower())
                except:
                    logging.warning(
                        f'Failed to retrieve ticker for {company[1]}')

                try:
                    _ = company[6].split("pd_percent'>")[1].split("</div")[0]
                except:
                    logging.warning(
                        f'Failed to retrieve percent change for {company[1]}')

                if _ == "-":
                    pct_list.append(0)
                else:
                    try:
                        pct_list.append(
                            _.split("%</span>")[0].split(">")[1])
                    except:
                        logging.warning(
                            f'Failed to extract percent change for {company[1]}')

        try:
            Prices.df_unfiltered = pd.DataFrame(
                {'name': company_list, 'ticker': ticker_list, 'pct': pct_list})
        except:
            logging.critial(f'Failed to create df_unfiltered')
            return

        try:
            Prices.df_fixed_keys = pd.read_csv(
                current_dir + 'no_fixed_keys.csv')
        except:
            logging.critial(f'Failed to read and create df_fixed_keys')
            return

        try:
            Prices.df_fixed_keys['mcap'] = pd.to_numeric(
                Prices.df_fixed_keys['mcap'])
        except:
            logging.critial(
                f'Failed to convert mcap to numeric in df_fixed_keys')
            return

        try:
            Prices.df_filtered = Prices.df_unfiltered.merge(
                Prices.df_fixed_keys, on='ticker')
        except:
            logging.critial(
                f'Failed to merge df_fixed_keys into df_filtered on ticker')
            return

        try:
            Prices.df_filtered = Prices.df_filtered[Prices.df_filtered['mcap'] > 1000000000]
        except:
            logging.critial(
                f'Failed to filter out companies with market cap below 1 b nok')
            return

    @staticmethod
    def calculate_change(index_list):
        today, yesterday = index_list[len(
            index_list)-1], index_list[len(index_list)-2]

        if type(today) == list:
            today = index_list[len(index_list)-1][1]

        if type(yesterday) == list:
            yesterday = index_list[len(index_list)-2][1]

        change = round(((today / yesterday) - 1) * 100, 2)

        return change

    @staticmethod
    def write_cap(file_name, change):
        with open(current_dir + file_name, 'a') as fd:
            fd.write(str(change) + ',' +
                     str(current_date) + '\n')

    def fetch_osebx():
        try:
            osebx = rq.get(urls['no']['index']).json()['points']
        except:
            logging.critial(
                f"Failed to fetch OSEBX change. OSEBX API response: {osebx.status_code}")
            return

        try:
            change = Prices.calculate_change(osebx)
        except:
            logging.critial(
                f"Failed to calculate OSEBX change")
            return
        try:
            Prices.write_cap('osebx.csv', change)
            print('OSEBX fetched')
        except:
            logging.critial(
                f"Failed to write OSEBX change")
            return

    def main():
        try:
            post_data = urls['no']['prices']
            post_data['headers']['Cookie'] = post_data['headers']['Cookie'].replace(
                '@time@', current_time)
        except:
            logging.critial(
                f'Failed to fetch prices from urls json and insert time ({current_time}) into post_data')
            return

        try:
            response_raw = rq.post(
                url=post_data['url'], headers=post_data['headers'], data=post_data['data'])
            response = response_raw.json()
            logging.debug(f'Prices API response: {response_raw.status_code}')
        except:
            logging.critical(
                f'Failed to retrieve prices for Norway. Prices API response: {response_raw.status_code}')
            return

        Prices.populate_df(response)

        export_name = current_dir + str(current_date) + ' no_prices.csv'

        logging.debug(f'Export name: {export_name}')

        try:
            Prices.df_filtered.to_csv(export_name, index=False)
            logging.info(f'Export successfull, see {export_name}')
            print(f'Export successfull, see {export_name}')
        except:
            logging.critical(f'Export of Norwegian articles failed')
            return

        Prices.fetch_osebx()
        logging.info(f'OSEBX return successfully saved')
        print(f'Export successfull, see osebx.csv')


class Train:
    df_prices = pd.DataFrame()
    temp_stock_version = 0

    def company_loop(article):
        temp_list = []
        try:
            for company_name in list(Train.df_prices.index.values):
                company_count = sum(1 for _ in re.finditer(
                    r'\b%s\b' % re.escape(company_name), article))
                temp_list.append((company_name, company_count))

            sorted_list = sorted(temp_list, key=lambda x: x[1], reverse=True)
        except:
            logging.critical(
                f'Failed to extract a list of company mentions count')
            return

        if sorted_list[0][1] > 3:
            return sorted_list[0][0]

        return None

    def fetch_stock_return(stock):
        if sum(Train.df_prices.index == stock) > 1:
            stock_return = round(
                Train.df_prices.loc[stock]['pct'][Train.temp_stock_version], 2)
        else:
            stock_return = round(
                Train.df_prices.loc[stock]['pct'], 2)

        return stock_return

    def fetch_stock_beta(stock):
        if sum(Train.df_prices.index == stock) > 1:
            stock_beta = Train.df_prices.loc[stock]['beta'][Train.temp_stock_version]
        else:
            stock_beta = Train.df_prices.loc[stock]['beta']

        return stock_beta

    def fetch_index_return(file_path):
        df_index = pd.read_csv(current_dir + file_path)
        df_index.set_index('date', inplace=True)

        return df_index.loc[current_date]['change']

    def main():
        try:
            df_articles = pd.read_csv(
                current_dir + str(current_date) + ' no_articles.csv').dropna()
            df_articles.set_index('date', inplace=True)
            logging.info(f'Reading csv for df_articles')
        except:
            logging.critical(f'Failed to read csv for df_articles')
            return

        try:
            Train.df_prices = pd.read_csv(
                current_dir + str(current_date) + ' no_prices.csv')
            Train.df_prices.set_index('name', inplace=True)
            logging.info(f'Reading csv for df_prices')
        except:
            logging.critical(f'failed to read csv for df_prices')
            return

        try:
            df_training_data = pd.read_csv(
                current_dir + 'no_training_data.csv')
            logging.info(f'Reading csv for df_training_data')
        except:
            logging.error(f'Error when reading csv for df_training_data')

        temp_date_list = []
        temp_company_list = []
        temp_article_list = []
        temp_grade_list = []

        for article in df_articles['article']:
            temp_stock_name = Train.company_loop(article)
            Train.temp_stock_version = 0

            if temp_stock_name != None:
                temp_stock_return = Train.fetch_stock_return(
                    temp_stock_name)
                temp_stock_beta = Train.fetch_stock_beta(temp_stock_name)
                temp_index_return = Train.fetch_index_return('osebx.csv')
                temp_stock_risk_adjusted_return = float(
                    temp_index_return) * float(temp_stock_beta)
                temp_stock_net_return = float(
                    temp_stock_return) - float(temp_stock_risk_adjusted_return)
                temp_category = 0

                if temp_stock_net_return > 6:
                    temp_category = 5
                elif temp_stock_net_return > 4:
                    temp_category = 4
                elif temp_stock_net_return > 2:
                    temp_category = 3
                elif temp_stock_net_return > 0:
                    temp_category = 2
                else:
                    temp_category = 1

                temp_date_list.append(str(current_date))
                temp_company_list.append(temp_stock_name)
                temp_article_list.append(article)
                temp_grade_list.append(temp_category)

        df_temp = pd.DataFrame({'date': temp_date_list, 'company': temp_company_list,
                               'article': temp_article_list, 'grade': temp_grade_list})
        df_training_data = pd.concat(
            [df_training_data, df_temp], ignore_index=True)

        logging.info(
            f'Number of trainable articles exported: {len(temp_company_list)}')

        for i in range(1, 6):
            try:
                logging.info(
                    f"of which {df_temp.groupby('grade').count()['company'].loc[i]} articles with grade {i}")
            except:
                logging.info(f"of which 0 articles with grade {i}")

        export_name = current_dir + 'no_training_data.csv'

        df_training_data.to_csv(export_name, index=False)

        logging.info(f'Export successfull, see {export_name}')
        print(f'Export successfull, see {export_name}')


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            print('Wrong amount of arguments given.')
            print(
                f'Usage: python {sys.argv[0]} <stage>. Eg: python {sys.argv[0]} articles. Run python {sys.argv[0]} help for stages.')
            logging.error('Wrong amount of sys arguments given')
            sys.exit(1)

        stage = sys.argv[1].lower()

        logging.debug(f'Sys argument given: {stage}')

        if stage == 'help':
            print('The following stages are available: articles, prices and train.')
        elif stage == 'articles':
            Articles.main()
        elif stage == 'prices':
            Prices.main()
        elif stage == 'train':
            Train.main()
        else:
            print('Provided stage not found.')
    except Exception as e:
        logging.error(f'Failed to run stage {stage}: ' + str(e))
