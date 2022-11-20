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

index_dict = {
    'large cap': 'largecap.csv',
    'mid cap': 'midcap.csv',
    'small cap': 'smallcap.csv',
    'ngm': 'ngm.csv',
    'first north': 'firstnorth.csv'
}

current_dir = os.path.expanduser('~') + '/programmering/wire_interpreter/'
urls_json = open(current_dir + 'urls.json')
urls = json.load(urls_json)
urls_json.close()


class Articles:
    blacklisted_words = ['återköp av egna', 'kallelse till', 'bolagsstämma',
                         'bjuder in', 'presentera delårsrapporten', 'inbjudan till',
                         'publicerar', 'årsstämma', 'valberedning']

    def get_lang_detector(nlp, name):
        return LanguageDetector()

    def main():
        site = newspaper.build(urls['articles']['articles'])
        site_urls = site.article_urls()
        articles = []

        nlp = spacy.load("en_core_web_sm")
        Language.factory('language_detector', func=Articles.get_lang_detector)
        nlp.add_pipe('language_detector', last=True)

        for article_url in site_urls:
            article = newspaper.Article(article_url)
            article.download()
            article.parse()
            if len(article.text) > 300:
                if not any(wrd in article.text[:300].lower() for wrd in Articles.blacklisted_words):
                    doc = nlp(article.text)

                    if doc._.language['language'] == 'sv' and doc._.language['score'] > 0.8:
                        articles.append(article.text.lower())

        df_articles = pd.DataFrame(articles, columns=['article'])
        df_articles['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        export_name = current_dir + datetime.datetime.now().strftime('%Y-%m-%d') + \
            ' articles.csv'

        df_articles.to_csv(export_name, index=False)

        print(f'Export successfull, see {export_name}')


class Prices:
    markets = ['small cap', 'mid cap', 'large cap', 'first north', 'ngm']

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
                     str((datetime.datetime.now()-datetime.timedelta(1)).strftime(
                         '%Y-%m-%d')) + '\n')

    def fetch_firstnorth():
        firstnorth = rq.get(urls['articles']['first north']).json()['points']
        change = Prices.calculate_change(firstnorth)
        Prices.write_cap('firstnorth.csv', change)

        print('first north fetched')

    def fetch_largecap():
        largecap = rq.get(urls['articles']['large cap']).json()['points']
        change = Prices.calculate_change(largecap)
        Prices.write_cap('largecap.csv', change)

        print('large cap fetched')

    def fetch_midcap():
        midcap = rq.get(urls['articles']['mid cap']).json()['points']
        change = Prices.calculate_change(midcap)
        Prices.write_cap('midcap.csv', change)

        print('mid cap fetched')

    def fetch_smallcap():
        smallcap = rq.get(urls['articles']['small cap']).json()['points']
        change = Prices.calculate_change(smallcap)
        Prices.write_cap('smallcap.csv', change)

        print('small cap fetched')

    def fetch_ngm():
        ngm = rq.get(urls['articles']['ngm']).json()['axisPx']
        change = Prices.calculate_change(ngm)
        Prices.write_cap('ngm.csv', change)

        print('ngm fetched')

    def main():
        response = rq.get(urls['articles']['prices'] + str(time.time()
                                                           ).split('.')[0]).json()['data']
        df_unfiltered = pd.json_normalize(response)
        df_unfiltered['list'] = df_unfiltered['list'].str.lower()
        df_unfiltered['name'] = df_unfiltered['name'].str.lower()
        df_filtered = df_unfiltered[df_unfiltered['list'].isin(
            Prices.markets)]
        df_filtered['borsvarde'] = pd.to_numeric(df_filtered['borsvarde'])
        df_filtered = df_filtered[df_filtered['borsvarde'] > 1000000000]
        df_filtered = df_filtered[df_filtered['lastprice'].notna()]

        export_name = current_dir + str((datetime.datetime.now()-datetime.timedelta(1)).strftime(
            '%Y-%m-%d')) + ' prices.csv'

        df_filtered.to_csv(export_name, index=False)

        print(f'Export successfull, see {export_name}')

        Prices.fetch_firstnorth()
        Prices.fetch_largecap()
        Prices.fetch_midcap()
        Prices.fetch_smallcap()
        Prices.fetch_ngm()

        print(f'Export successfull, see index files')


class Train:
    df_prices = pd.DataFrame()
    index_grade = {
        'large cap': 5,
        'mid cap': 4,
        'small cap': 3,
        'ngm': 1,
        'first north': 2
    }
    temp_stock_version = 0

    def company_loop(article):
        temp_list = []

        for company_name in list(Train.df_prices.index.values):
            company_count = sum(1 for _ in re.finditer(
                r'\b%s\b' % re.escape(company_name), article))
            temp_list.append((company_name, company_count))

        sorted_list = sorted(temp_list, key=lambda x: x[1], reverse=True)

        if sorted_list[0][1] > 0:
            return sorted_list[0][0]
        return None

    def fetch_stock_return(stock):
        if sum(Train.df_prices.index == stock) > 1:
            stock_return = round(
                Train.df_prices.loc[stock]['diff1dprc'][Train.temp_stock_version], 2)
        else:
            stock_return = round(
                Train.df_prices.loc[stock]['diff1dprc'], 2)

        return stock_return

    def fetch_stock_index(stock):
        if sum(Train.df_prices.index == stock) > 1:
            grade = []

            for company in Train.df_prices.loc[stock]['list']:
                grade.append(Train.index_grade[company])

            index_name = list(Train.index_grade.keys())[list(
                Train.index_grade.values()).index(max(grade))]
            Train.temp_stock_version = grade.index(max(grade))
        else:
            index_name = Train.df_prices.loc[stock]['list']

        return index_name

    def fetch_stock_beta(stock):
        if sum(Train.df_prices.index == stock) > 1:
            stock_beta = Train.df_prices.loc[stock]['beta'][Train.temp_stock_version]
        else:
            stock_beta = Train.df_prices.loc[stock]['beta']

        if stock_beta == 0:
            stock_beta = 1

        return stock_beta

    def fetch_index_return(file_path):
        df_index = pd.read_csv(current_dir + file_path)
        df_index.set_index('date', inplace=True)

        return df_index.loc[(datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y-%m-%d')]['change']

    def main():
        df_articles = pd.read_csv(current_dir + (datetime.datetime.now()-datetime.timedelta(1)).strftime(
            '%Y-%m-%d') + ' articles.csv').dropna()
        df_articles.set_index('date', inplace=True)
        Train.df_prices = pd.read_csv(current_dir +
                                      (datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y-%m-%d') + ' prices.csv')
        Train.df_prices.set_index('name', inplace=True)
        df_training_data = pd.read_csv('training_data.csv')
        temp_date_list = []
        temp_company_list = []
        temp_article_list = []
        temp_grade_list = []

        for article in df_articles['article']:
            temp_stock_name = Train.company_loop(article)
            Train.temp_stock_version = 0

            if temp_stock_name != None:
                temp_index_name = Train.fetch_stock_index(temp_stock_name)
                temp_stock_return = Train.fetch_stock_return(
                    temp_stock_name)
                temp_stock_beta = Train.fetch_stock_beta(temp_stock_name)
                temp_index_return = Train.fetch_index_return(
                    index_dict[temp_index_name])
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

                temp_date_list.append(str((datetime.datetime.now()-datetime.timedelta(1)).strftime(
                    '%Y-%m-%d')))
                temp_company_list.append(temp_stock_name)
                temp_article_list.append(article)
                temp_grade_list.append(temp_category)

        df_temp = pd.DataFrame({'date': temp_date_list, 'company': temp_company_list,
                               'article': temp_article_list, 'grade': temp_grade_list})
        df_training_data = pd.concat(
            [df_training_data, df_temp], ignore_index=True)

        export_name = current_dir + 'training_data.csv'

        df_training_data.to_csv(export_name, index=False)

        print(f'Export successfull, see {export_name}')


class Debug_history:
    df_prices = pd.DataFrame()
    index_grade = {
        'large cap': 5,
        'mid cap': 4,
        'small cap': 3,
        'ngm': 1,
        'first north': 2
    }
    temp_stock_version = 0

    def company_loop(article):
        temp_list = []

        for company_name in list(Debug_history.df_prices.index.values):
            company_count = sum(1 for _ in re.finditer(
                r'\b%s\b' % re.escape(company_name), article))
            temp_list.append((company_name, company_count))

        sorted_list = sorted(temp_list, key=lambda x: x[1], reverse=True)

        if sorted_list[0][1] > 0:
            return sorted_list[0][0]
        return None

    def fetch_stock_return(stock):
        if sum(Debug_history.df_prices.index == stock) > 1:
            stock_return = round(
                Debug_history.df_prices.loc[stock]['diff1dprc'][Debug_history.temp_stock_version], 2)
        else:
            stock_return = round(
                Debug_history.df_prices.loc[stock]['diff1dprc'], 2)

        return stock_return

    def fetch_stock_index(stock):
        if sum(Debug_history.df_prices.index == stock) > 1:
            grade = []

            for company in Debug_history.df_prices.loc[stock]['list']:
                grade.append(Debug_history.index_grade[company])

            index_name = list(Debug_history.index_grade.keys())[list(
                Debug_history.index_grade.values()).index(max(grade))]
            Debug_history.temp_stock_version = grade.index(max(grade))
        else:
            index_name = Debug_history.df_prices.loc[stock]['list']

        return index_name.lower()

    def fetch_stock_beta(stock):
        if sum(Debug_history.df_prices.index == stock) > 1:
            stock_beta = Debug_history.df_prices.loc[stock]['beta'][Debug_history.temp_stock_version]
        else:
            stock_beta = Debug_history.df_prices.loc[stock]['beta']

        if stock_beta == 0:
            stock_beta = 1

        return stock_beta

    def fetch_index_return(file_path, hist_date):
        df_index = pd.read_csv(current_dir + file_path)
        df_index.set_index('date', inplace=True)

        return df_index.loc[hist_date]['change']

    def main():
        history_list = ['2022-10-20', '2022-10-21', '2022-10-24', '2022-10-25', '2022-10-26', '2022-10-27', '2022-10-28', '2022-10-31', '2022-11-01', '2022-11-02',
                        '2022-11-03', '2022-11-04', '2022-11-07', '2022-11-08', '2022-11-09', '2022-11-10', '2022-11-11', '2022-11-14', '2022-11-16', '2022-11-17', '2022-11-18']
        df_training_data = pd.read_csv('training_data.csv')

        for hist_date in history_list:
            df_articles = pd.read_csv(
                current_dir + str(hist_date) + ' articles.csv').dropna()
            df_articles.set_index('date', inplace=True)
            Debug_history.df_prices = pd.read_csv(
                current_dir + str(hist_date) + ' prices.csv')
            Debug_history.df_prices.set_index('name', inplace=True)

            temp_date_list = []
            temp_company_list = []
            temp_article_list = []
            temp_grade_list = []

            for article in df_articles['article']:
                temp_stock_name = Debug_history.company_loop(article)
                Debug_history.temp_stock_version = 0

                if temp_stock_name != None:
                    temp_index_name = Debug_history.fetch_stock_index(
                        temp_stock_name)
                    temp_stock_return = Debug_history.fetch_stock_return(
                        temp_stock_name)
                    temp_stock_beta = Debug_history.fetch_stock_beta(
                        temp_stock_name)
                    temp_index_return = Debug_history.fetch_index_return(
                        index_dict[temp_index_name.lower()], str(hist_date))
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

                    temp_date_list.append(str(hist_date))
                    temp_company_list.append(temp_stock_name)
                    temp_article_list.append(article)
                    temp_grade_list.append(temp_category)

            df_temp = pd.DataFrame({'date': temp_date_list, 'company': temp_company_list,
                                    'article': temp_article_list, 'grade': temp_grade_list})
            df_training_data = pd.concat(
                [df_training_data, df_temp], ignore_index=True)

        export_name = current_dir + 'training_data.csv'

        df_training_data.to_csv(export_name, index=False)

        print(f'Export successfull, see {export_name}')


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Wrong amount of arguments given.')
        print(
            f'Usage: python {sys.argv[0]} <stage>. Eg: python {sys.argv[0]} articles. Run python {sys.argv[0]} help for stages.')
        sys.exit(1)

    stage = sys.argv[1].lower()

    if stage == 'help':
        print('The following stages are available: articles, prices and train.')
    elif stage == 'articles':
        Articles.main()
    elif stage == 'prices':
        Prices.main()
    elif stage == 'train':
        Train.main()
    elif stage == 'debug_history':
        Debug_history.main()
    else:
        print('Provided stage not found.')
