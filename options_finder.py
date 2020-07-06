import json
import time
import requests

import yfinance as yf

MULTIPLIER = 2
COMPANIES = [
    'MSFT', 'BA', 'BKNG', 'NVDA', 'UNH', 'WMT', 'HD', 'MCD', 'AMZN', 'GOOGL',
    'JPM', 'SHOP', 'SPOT', 'ULTA', 'BIDU', 'BYND', 'ROKU', 'ZM', 'TSLA'
]

nearest_exp_date = yf.Ticker("MSFT").options[0]


def _append_option_data(storage, row_data, direction):
    if row_data['volume'] > row_data['openInterest'] * MULTIPLIER:
        storage[direction].append({
            'strike': row_data['strike'],
            'volume': row_data['volume'],
            'openInterest': row_data['openInterest'],
            'change': row_data['change'],
            'percentChange': row_data['percentChange']
        })


def _process_companies_options(companies):
    result = []
    for company in companies:
        try:
            company_data = _process_company(company)
            if company_data:
                result.append(company_data)
        except ValueError:
            print(f'{company} was skipped due to Exception')
        except IndexError:
            print(f'{company} do not trade Options')

    return result


def _process_company(company):
    ticker = yf.Ticker(company)
    opt = ticker.option_chain(nearest_exp_date)

    data = {
        'calls': [],
        'puts': []
    }

    for _, row in opt.calls.iterrows():
        _append_option_data(data, row, 'calls')

    for _, row in opt.puts.iterrows():
        _append_option_data(data, row, 'puts')

    if data['calls'] or data['puts']:
        calls = sorted(data['calls'], key=lambda i: i['volume'],
                       reverse=True)
        puts = sorted(data['puts'], key=lambda i: i['volume'],
                      reverse=True)

        price_change_direction = _calc_price_change_direction(calls, puts)
        volume_direction = _calc_volume_direction(calls, puts)

        if price_change_direction \
                and _verify_signal(price_change_direction, volume_direction):
            print(f'Got data for {company}')
            return {
                'company': company,
                'volume_direction': volume_direction,
                'price_change_direction': price_change_direction,
                'options': {
                    'calls': calls[:5],
                    'puts': puts[:5]
                }
            }
        return None


def _verify_signal(price_change_direction, volume_direction):
    if volume_direction == 'SELL' \
            and price_change_direction['number_of_positive_puts'] > 0:
        return True
    elif volume_direction == 'BUY' \
            and price_change_direction['number_of_positive_calls'] > 0:
        return True
    return False


def _calc_volume_direction(calls, puts):
    sum_puts = sum(i['volume'] for i in puts)
    sum_calls = sum(i['volume'] for i in calls)

    if sum_calls > sum_puts:
        volume_direction = 'BUY'
    elif sum_calls < sum_puts:
        volume_direction = 'SELL'
    else:
        volume_direction = 'EQUAL'
    return volume_direction


def _calc_price_change_direction(calls, puts):
    number_of_positive_puts = 0
    number_of_positive_calls = 0
    for call in calls[:5]:
        if call['percentChange'] > 0:
            number_of_positive_calls += 1

    for put in puts[:5]:
        if put['percentChange'] > 0:
            number_of_positive_puts += 1

    if not number_of_positive_calls and not number_of_positive_puts:
        return None

    return {
        'number_of_positive_calls': number_of_positive_calls,
        'number_of_positive_puts': number_of_positive_puts
    }


def _jsonify(data_to_format):
    return json.dumps(data_to_format, indent=2)


def _write_to_json_file(records, date):
    with open(f'options-{date}.json', 'w+') as outfile:
        json.dump(records, outfile, indent=2)


def _get_elapsed_time():
    start_time = time.time()
    return '%.3fs' % (time.time() - start_time)


def _send_get_request(url):
    return json.loads(requests.get(url).text)


def _get_most_active_stocks():
    url = 'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&lang=en-US&region=US&scrIds=most_actives&count=100'
    res = _send_get_request(url)
    quotes_data = res['finance']['result'][0]['quotes']
    return [i['symbol'] for i in quotes_data]


def _get_analytics_trend(company):
    url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{company}?modules=recommendationTrend'
    res = _send_get_request(url)
    return res['quoteSummary']['result'][0]['recommendationTrend']['trend']


def _get_upgrade_downgrade_history(company):
    url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{company}?modules=upgradeDowngradeHistory'
    res = _send_get_request(url)
    return res['quoteSummary']['result'][0]['upgradeDowngradeHistory']['history']


if __name__ == "__main__":
    print(f'=== Started ===')
    most_active = set(_get_most_active_stocks() + COMPANIES)
    print(f'Total Active companies: {len(most_active)}')
    options_result = _process_companies_options(most_active)
    _write_to_json_file(options_result, nearest_exp_date)
    print(f'Scrapping has been finished. '
          f'Total filtered results: {len(options_result)}. '
          f'Elapsed time: {_get_elapsed_time()}')


# https://stackoverflow.com/questions/44030983/yahoo-finance-url-not-working/45907754