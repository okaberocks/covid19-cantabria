#!/usr/bin/env python

import requests
from bs4 import BeautifulSoup
import re

url = "https://www.scsalud.es/coronavirus"
response = requests.get(url)
# parse html
page = str(BeautifulSoup(response.content, features="html.parser"))


def getURL(page):
    """

    :param page: html of web page (here: Python home page) 
    :return: urls in that page 
    """
    start_link = page.find("a href")
    if start_link == -1:
        return None, 0
    start_quote = page.find('"', start_link)
    end_quote = page.find('"', start_quote + 1)
    url = page[start_quote + 1: end_quote]
    return url, end_quote

historico_url='https://www.scsalud.es'
municipal_url='https://www.scsalud.es'
edad_url='https://www.scsalud.es'
while True:
    url, n = getURL(page)
    page = page[n:]
    if url:
        x = re.search(".*covid.*historico.*\.csv", url) 
        if x:
            historico_url += url
        x = re.search(".*covid.*municipal.*\.csv", url) 
        if x:
            municipal_url += url
        x = re.search(".*covid.*edad.*\.csv", url) 
        if x:
            edad_url += url
    else:
        break
