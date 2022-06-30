import asyncio
import json
import re
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
from utils import get_db_session
from base_parser import BaseParser
from notifications import notify_slack_failure, notify_slack_success
from db_operations import get_seasonal_sale_status
from redis_utils import get_redis_client

USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
)


class NordstromRackParser(BaseParser):
    timeout = 60
    redis = get_redis_client()
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*', 'Connection': 'keep-alive',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cookie': ''
    }
    COOKIE_TEMPLATE = 'Od34bsR56={cookie};'
    RE_DATA = r'window\.__INITIAL_CONFIG__ = (?P<data>.*)</'
    product_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9,uk-UA;q=0.8,uk;q=0.7',
        'cookie': '',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
    }
    base_url = 'https://www.nordstromrack.com/'

    async def execute_scraping(self, json_data):
        result = []
        for item in json_data['productResults']['productsById'].values():
            product_id = item['id']
            prices = self.get_prices(item['pricesById'])

            if not prices:
                continue

            product_url, alternative_product_url = self.get_product_url(item)
            product_data = {
                'product_id': product_id,
                'brand': item.get('brandName') or 'N/A',
                'model': item.get('name', 'N/A'),
                'msrp': prices['comparable_value'],
                'current_price': prices['sale_price'],
                'discount': prices['total_savings'],
                'product_url': product_url,
                'alternative_product_url': alternative_product_url,
            }
            result.append(product_data)
        return result

    def get_product_url(self, item):
        product_url = alternative_product_url = self.check_url(item['productPageUrl'])
        return product_url, alternative_product_url

    def get_prices(self, item):
        prices = {}
        discounted_prices = item.get('promotion') or item.get('clearance') or item['regular']
        if not item.get('compareat'):
            return None

        comparable_value = self.get_clear_price(item['compareat']['maxItemPrice'])
        sale_price = self.get_clear_price(discounted_prices['minItemPrice'])
        if self.is_seasonal_sale[0]:
            sale_price = round(sale_price * 0.75, 2)
        total_savings = (comparable_value - sale_price) / comparable_value
        prices.update({
            'comparable_value': self.get_clear_price(comparable_value),
            'sale_price': sale_price,
            'total_savings': total_savings
        })
        return prices

    async def fetch_soup(self, session, request_url, proxy=None, parse_product=None, url_name=None):
        proxy = proxy or self.get_random_proxy()
        headers = self.product_headers

        if 'clearance/women/clothing' in request_url.lower() or 'com/clearance' not in request_url.lower():
            cookie = self.redis.hgetall('COOKIE').get('Od34bsR56')
            headers['cookie'] = cookie

        async with session.get(
                url=request_url,
                proxy=proxy,
                proxy_auth=self.proxy_auth,
                headers=headers,
        ) as response:
            try:
                resp_text = await response.text()
                bs = BeautifulSoup(resp_text, 'lxml')
                if parse_product is None:
                    return self.get_json_category_data(bs, url_name)

                return bs

            except (TimeoutError, asyncio.TimeoutError):
                print(f'TimeoutError!\n|Url: {url_name} .\n|Proxy: {proxy}\n')
                return None

            except Exception as error:
                print(f'Incorrect response, please check.\n|Url: {url_name} .\n|Proxy: {proxy}\n|{str(error)}')
                return None

    async def gather_all_products(self, source_item, _, **kwargs):
        self.redis = get_redis_client()
        if ('clearance/women/clothing' in source_item.url.lower() or
            'com/clearance' not in source_item.url.lower()) and not self.redis.hgetall('COOKIE').get('Od34bsR56'):
            print("No cookie, failed to parse")
            return [], []

        session = get_db_session()
        self.is_seasonal_sale = get_seasonal_sale_status(session)
        session.close()
        all_products = []
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as async_session:
                before_request = datetime.utcnow()
                proxy = self.get_random_proxy()
                request_url = source_item.url
                response = await self.fetch_soup(
                    async_session,
                    request_url,
                    proxy,
                    url_name=source_item.name,
                )
                if not response:
                    after_request = datetime.utcnow()
                    total_duration = round((after_request - before_request).total_seconds(), 2)
                    notify_slack_failure(source_item.name + f' | Duration {total_duration} seconds')
                    return all_products, len(all_products)

                before_scraping = datetime.utcnow()
                all_products = await self.execute_scraping(response)
                after_request = datetime.utcnow()
                total_duration = round((after_request - before_request).total_seconds(), 2)
                notify_slack_success(source_item.name, len(all_products), total_duration)
                print(f'Requesting url {source_item.name}, Total duration {total_duration}, '
                      f'time for scraping {after_request - before_scraping}', f"Found {len(all_products)} products.")
                return all_products, len(all_products)
        except asyncio.TimeoutError:
            notify_slack_failure(source_item.name + f' | TimeoutError | Duration {self.timeout} seconds')
            return None, None

    def get_json_category_data(self, bs, url_name=None):
        for script in bs.select('script'):
            script_text = str(script)
            if '__INITIAL_CONFIG__' in script_text:
                data = re.search(self.RE_DATA, script_text)
                if data:
                    try:
                        bs.select('getting json')
                        return json.loads(data.group('data'))
                    except json.decoder.JSONDecodeError:
                        print(f'Incorrect JSON format for URL {url_name}')

        if url_name:
            print(f'Could not find JSON data with categories for URL {url_name}')
        return None

    async def parse_single_product(self, product_url):
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as async_session:
            response = await self.fetch_soup(async_session, product_url, parse_product=True)
            if not response:
                print(f'Got empty response while requesting URL {product_url}')
                return None

            product = await self.execute_product_scraping(response, product_url)
            return product

    async def execute_product_scraping(self, response, product_url):
        print(f'Requesting Product URL {product_url}')
        json_data = self.get_sku_data(response)['productPage']
        selected_color = json_data.get('selectedColor', '')
        product_data = json_data['product']
        color_data = [color for color in product_data['colors'] if color['value'] == selected_color]
        if not color_data:
            return None

        active_color = color_data[0]
        size_data = [size for size in active_color['sizes'] if size['isAvailable']]
        if not size_data:
            return None

        active_sku = []
        for size in size_data:
            active_sku.append({
                'sku': size['sku'],
                'size': size['value'],
                'quantity': size['lowQuantity'],
            })
        active_sku_to_print = '\n'.join(json.dumps(item) for item in active_sku)
        print(f'Found following active SKU for URL {product_url}:\n{active_sku_to_print}')
        product_id = str(product_data['styleId'])
        color_id = active_color['value'].replace(' ', '_')
        product = {
            'product_id': '_'.join(item.lower() for item in (product_id, color_id)),
            'brand': product_data.get('brandName') or 'N/A',
            'model': product_data.get('name', 'N/A'),
            'product_url': product_url,
            'alternative_product_url': product_url,
        }
        return product

    @staticmethod
    def get_sku_data(soup):
        for script in soup.select('script'):
            text = script.text
            if '__INITIAL_STATE__' not in text:
                continue

            return json.loads(re.search(r'__INITIAL_STATE__\s*=\s*(?P<data>{.+\})', text).group('data'))
