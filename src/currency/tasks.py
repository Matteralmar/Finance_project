import logging
from datetime import date

from celery import shared_task
import requests
from django.utils.timezone import now
from requests.exceptions import HTTPError


@shared_task
def parse_privatebank():
    from .models import Currency

    url = 'https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=5'
    response = requests.get(url)
    response.raise_for_status()

    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        # TODO: log http_err
        logging.error(http_err, exc_info=True)
    except Exception as err:
        # TODO: log err
        logging.error(err, exc_info=True)

    currency_map = {
        'USD': 1,
        'EUR': 2,
    }

    data = response.json()
    for row in data:
        if row['ccy'] in currency_map:
            buy = row['buy']
            sale = row['sale']
            ccy = currency_map[row['ccy']]
            cr_last = Currency.objects.filter(currency=ccy, source=1).last()
            is_currency_empty = cr_last is None
            if is_currency_empty:
                is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
                if is_currency_new_value:
                    Currency.objects.create(currency=ccy, source=1, buy=buy, sale=sale)


@shared_task
def parse_monobank():
    from .models import Currency

    url = 'https://api.monobank.ua/bank/currency'
    response = requests.get(url)
    response.raise_for_status()

    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        # TODO: log http_err
        logging.error(http_err, exc_info=True)
    except Exception as err:
        # TODO: log err
        logging.error(err, exc_info=True)

    currency_map = {
        840: 1,
        978: 2,
    }

    data = response.json()
    for row in data:
        if row['currencyCodeA'] in currency_map:
            if row['currencyCodeA'] == 840:
                buy = row['rateBuy']
                sale = row['rateSell']
                ccy = currency_map[row['currencyCodeA']]
                cr_last = Currency.objects.filter(currency=ccy, source=2).last()
                is_currency_empty = cr_last is None
                if is_currency_empty:
                    is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
                    if is_currency_new_value:
                        Currency.objects.create(currency=ccy, source=2, buy=buy, sale=sale)
            if row['currencyCodeA'] == 978:
                buy = row['rateBuy']
                sale = row['rateSell']
                ccy = currency_map[row['currencyCodeA']]
                cr_last = Currency.objects.filter(currency=ccy, source=2).last()
                is_currency_empty = cr_last is None
                if is_currency_empty:
                    is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
                    if is_currency_new_value:
                        Currency.objects.create(currency=ccy, source=2, buy=buy, sale=sale)

@shared_task
def parse_vkurse():
    from .models import Currency

    url = 'http://vkurse.dp.ua/course.json'
    response = requests.get(url)
    response.raise_for_status()

    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        # TODO: log http_err
        logging.error(http_err, exc_info=True)
    except Exception as err:
        # TODO: log err
        logging.error(err, exc_info=True)

    data = response.json()
    if data['Dollar']:
        buy = data['Dollar']['buy']
        sale = data['Dollar']['sale']
        cr_last = Currency.objects.filter(currency=1, source=3).last()
        is_currency_empty = cr_last is None
        if is_currency_empty:
            is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
            if is_currency_new_value:
                Currency.objects.create(currency=1, source=3, buy=buy, sale=sale)
    if data['Euro']:
        buy = data['Euro']['buy']
        sale = data['Euro']['sale']
        cr_last = Currency.objects.filter(currency=2, source=3).last()
        is_currency_empty = cr_last is None
        if is_currency_empty:
            is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
            if is_currency_new_value:
                Currency.objects.create(currency=2, source=3, buy=buy, sale=sale)


@shared_task
def parse_yahoo():
    from .models import Currency
    from yahoofinancials import YahooFinancials

    currency_map = {
        'USD': 1,
        'EUR': 2,
    }
    currencies = ['USDUAH=X', 'EURUAH=X']
    for currency in currencies:
        if currency == 'USDUAH=X':
            yahoo_financials_currencies = YahooFinancials(currency)
            sale = yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['USDUAH=X']['prices'][0]['close']
            buy = yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['USDUAH=X']['prices'][0]['adjclose']
            ccy = currency_map[yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['USDUAH=X']['currency']]
            cr_last = Currency.objects.filter(currency=ccy, source=4).last()
            is_currency_empty = cr_last is None
            if is_currency_empty:
                is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
                if is_currency_new_value:
                    Currency.objects.create(currency=ccy, source=4, buy=buy, sale=sale)
        if currency == 'EURUAH=X':
            yahoo_financials_currencies = YahooFinancials(currency)
            sale = yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['EURUAH=X']['prices'][0]['close']
            buy = yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['EURUAH=X']['prices'][0]['adjclose']
            ccy = currency_map[yahoo_financials_currencies.get_historical_price_data(str(date.today()), str(date.today()), "daily")['EURUAH=X']['currency']]
            cr_last = Currency.objects.filter(currency=ccy, source=4).last()
            is_currency_empty = cr_last is None
            if is_currency_empty:
                is_currency_new_value = cr_last.buy != buy or cr_last.sale != sale
                if is_currency_new_value:
                    Currency.objects.create(currency=ccy, source=4, buy=buy, sale=sale)