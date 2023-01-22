"""Custom client handling, including turkish_lira_paritiesStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import Stream

import pandas as pd
from bs4 import BeautifulSoup
import sys
from datetime import datetime

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0'
ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'

class gold_tl_parity_stream(Stream):
    """Stream class for gold_tl_parity streams."""
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        date_range_list_formatted = list(map(
            lambda x : {'date_for_url' : x.strftime('%Y/%m/%d'),'date_for_output' : x.strftime('%Y-%m-%d')}
            ,pd.date_range(self.config.get('Minimum Date'),self.config.get('Maximum Date')).to_pydatetime().tolist()
            ))
        for dt in date_range_list_formatted:
            headers = {
                        'User-Agent' : USER_AGENT,
                        'Accept' : ACCEPT
                    }
            link = 'https://altin.in/arsiv/{date}'.format(
                date=dt['date_for_url']
            )
            content = requests.get(link , headers = headers).text
            soup = BeautifulSoup(content,features='html.parser')
            price = soup.find_all('li',{'title': 'Gram Altın - Alış'})[0].text
            yield { 'parity' : 'gold' , 'date' : dt['date_for_output'] , 'buying_price' : price }

class foreign_exchange_tl_parity_stream(Stream):
    def __init__(self, *args, **kwargs):
        super(Stream, self).__init__(*args, **kwargs)
        self.parity_name = 'N/A'
    """Stream class for foreign_exchange_tl_parity_stream streams."""
    def download_contents(self,investment_tool : str, year : str) -> str:
        investment_tool_mapping = {'dollar' : 'amerikan-dolari','euro' : 'euro'}
        if investment_tool not in investment_tool_mapping:
            sys.exit('Supplied investment tool is not scrapable from this website')
        else:
            investment_tool = investment_tool_mapping[investment_tool]
        headers = {
                    'User-Agent' : USER_AGENT,
                    'Accept' : ACCEPT
                }
        link = 'https://paracevirici.com/doviz-arsiv/merkez-bankasi/gecmis-tarihli-doviz/{year}/{investment_tool}'.format(
            investment_tool = investment_tool
            ,year = year
        )
        return requests.get(link , headers = headers).text

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        start_year = int(datetime.strftime(datetime.strptime(self.config.get('Minimum Date'),'%Y-%m-%d'),'%Y'))
        end_year = int(datetime.strftime(datetime.strptime(self.config.get('Maximum Date'),'%Y-%m-%d'),'%Y'))
        for year in range(start_year,end_year + 1,1):
            soup = BeautifulSoup(self.download_contents(self.parity_name, year ),features='html.parser')
            divs = soup.find_all("div", {"class": "row"})
            for div in divs:
                temp_date = div.find('p',{ 'class' : 'date' }).text
                if temp_date != 'TARİH':
                    buying_price = div.find('p',{'class' : 'price buy'}).text.replace(',','.')[:-2]
                    splitted_date = temp_date.split('-')
                    date = splitted_date[2] + '-' + splitted_date[1] + '-' +  splitted_date[0]
                    yield { 'parity' : self.parity_name , 'date' : date , 'buying_price' : buying_price }

class dollar_tl_parity_stream(foreign_exchange_tl_parity_stream):
    def __init__(self, *args, **kwargs):
        super(foreign_exchange_tl_parity_stream, self).__init__(*args, **kwargs)
        self.parity_name = 'dollar'

class euro_tl_parity_stream(foreign_exchange_tl_parity_stream):
    def __init__(self, *args, **kwargs):
        super(foreign_exchange_tl_parity_stream, self).__init__(*args, **kwargs)
        self.parity_name = 'euro'
