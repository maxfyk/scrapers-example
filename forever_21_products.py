import json
import re

from ******.base_scraper import BaseParser
from ******.custom_exception import (
    ProductErrorException,
    ProductUnavailableException,
    ProblematicPageException,
    HOME_PAGE_REDIRECTION,
)
from ******.utils import (
    fill_variant_selectors,
    process_description,
    generate_size_attribute,
    generate_color_attribute,
    fix_lazy_json,
    remove_url_query,
)

IMG_ID_LIST = [
    "1_front_750",
    "2_side_750",
    "3_back_750",
    "4_full_750",
    "5_detail_750",
    "6_flat_750",
    "7_additional_750"
]

JSON_RE = re.compile(r'dataLayer\.push\((?P<data>{.*?\})\);', re.DOTALL)


class Forever21Parser(BaseParser):
    base_url = 'https://www.forever21.com/'
    currency = 'USD'
    swatch_url_templ = 'https://www.forever21.com/dw/image/v2/BFKH_PRD/on/demandware.static/-/Sites-f21-master-catalog/default/dw5a400f37/sw_22/{}-{}.jpg?sw=500&amp;sh=750'
    img_url_template = 'https://www.forever21.com/images/{}/{}-{}.jpg'
    color_url_template = 'https://www.forever21.com/on/demandware.store/Sites-forever21-Site/en_US/Product-Variation?' \
                         'dwvar_{}_color={}&pid={}'

    def should_retry(self, exception, response):
        if exception or not response or 'e_product_detail_loaded' not in response:
            return True

        if 'Canada' in self.create_bs(response).title:
            return True

        return False

    @staticmethod
    def color_retries(exception, response):
        if exception or not response or '"Product-Variation"' not in response:
            return True

        return False

    async def scrape_full(self):
        self.soup = await self.parse_document(
            encoding='ISO-8859-1',
            should_retry=self.should_retry,
        )
        prod_json = self.get_prod_json()
        if not prod_json.get('variants'):
            raise ProductUnavailableException()

        product_info = {
            'name': prod_json['name'].strip(),
            'description': self.get_descriprion(),
            'category': [prod_json['category']],
            'brand': prod_json['brand'],
            'attributes': self.get_attributes(prod_json),
            'assets': await self.get_assets(prod_json),
            'variantSelectors': [],
        }
        return fill_variant_selectors(product_info)

    def get_prod_json(self):
        for script in self.soup.find_all('script'):
            if 'e_product_detail_loaded' in script.text:
                str_data = re.search(JSON_RE, script.text).group('data')
                string_to_replace = re.search(r'\'seasonTrend\': (?P<string>.*?),', str_data).group('string')
                if string_to_replace:
                    str_data = str_data.replace(string_to_replace, '\'\'')
                print(fix_lazy_json(str_data.replace('\\', ''))[6000:6100])
                return json.loads(fix_lazy_json(str_data.replace('\\', '')))['product']

        raise ProductErrorException

    def get_descriprion(self):
        description_data = self.soup.select_one('div.pdp__details-description-container.col-12')
        toggle_tag = description_data.select_one('div.toggle-box__content > style')
        if toggle_tag:
            toggle_tag.extract()
        return process_description(description_data)

    def get_attributes(self, prod_json):
        colors = []
        sizes = []
        item_code = prod_json['imageFilename'].split('-')[0]
        for variant in prod_json['variants']:
            color_id = variant['colorID']
            color = {
                'id': color_id,
                'name': variant['colorName'],
                'swatch': self.swatch_url_templ.format(item_code, color_id),
            }

            if color not in colors:
                colors.append(color)
            for size_var in variant['sizes']:
                if not size_var['sizeName']:
                    raise ProblematicPageException('no size names')

                size = {
                    'id': size_var['sizeID'],
                    'name': size_var['sizeName'].strip(),
                }
                if size not in sizes:
                    sizes.append(size)
        color_attr = generate_color_attribute(value_type='swatch')
        color_attr['values'] = colors
        size_attr = generate_size_attribute()
        size_attr['values'] = sizes
        return [color_attr, size_attr]

    async def get_assets(self, prod_json):
        assets = []
        pr_id = prod_json['id']
        color_tags = self.soup.select('div.product-attribute__contents--color button')
        color_urls = [self.color_url_template.format(pr_id, col['data-attr-value'], pr_id) for col in color_tags]
        color_requests = await self.scraper.browser.get_all(color_urls)
        color_soups = [json.loads(soup) for soup in color_requests]
        for variant, col in zip(color_soups, color_tags):
            images = [{'url': remove_url_query(self.check_url(image['url']))} for image in
                      variant['product']['images']['large']]
            if not images:
                raise ProductErrorException('NO ASSETS')

            assets.append({
                'images': images,
                'videos': [],
                'selector': {'color': [col['data-attr-value']]},
            })
        return assets

    async def scrape_availability(self):
        self.soup = await self.parse_document(
            encoding='ISO-8859-1',
            should_retry=self.should_retry,
        )
        product_data = {'variants': []}
        prod_json = self.get_prod_json()
        prod_id = prod_json.get('id')
        if not prod_id:
            raise ProductErrorException(HOME_PAGE_REDIRECTION)

        original_price = prod_json['originalPrice']
        mpn = self.soup.select_one('[data-pid]')
        for color_var in prod_json['variants']:
            color_id = color_var['colorID']
            for size_var in color_var['sizes']:
                size_id = size_var['sizeID']
                if not size_var['available'] or size_var['available'] == 'false':
                    continue

                variant = {
                    'cart': {},
                    'id': '_'.join([prod_id, color_id, size_id]),
                    'price': self.get_price(original_price, size_var),
                    'selection': {
                        'color': color_id,
                        'size': size_id,
                    },
                    'stock': {'status': 'in_stock'},
                }
                if mpn:
                    variant['mpn'] = mpn['data-pid']
                product_data['variants'].append(variant)
        if not product_data['variants']:
            raise ProductUnavailableException()

        return product_data

    def get_price(self, original_price, size_var):
        fmp = regular = size_var['price']
        if '-' not in original_price:
            fmp = original_price
        return {
            'currency': self.currency,
            'fmp': fmp,
            'regular': regular,
        }
