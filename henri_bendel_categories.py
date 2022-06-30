import re

import scrapy

from app.spiders.base import BaseSpider


BASE_URL = 'https://www.henribendel.com{}'
GET_PARAMETERS = '?sz=48&start={}&format=page-element'


class HenriBendelSpider(BaseSpider):
    name = 'henri_bendel'
    start_urls = ['https://www.henribendel.com/us/homepage']
    retry_num = 3

    def parse(self, response):
        category_tags = response.xpath('//nav[@id="navigation"]/ul[@class="menu-category level-1"]/li')
        for index, tag in enumerate(category_tags, 1):
            gen_cat_1 = self._make_category(
                name=tag.xpath('./a/text()').extract_first().strip(),
                url=self.make_full_url(tag.xpath('./a/@href').extract_first()),
                index=index,
            )
            response.meta['gen_cat'] = gen_cat_1
            yield from self.get_2nd_level_categories(response, tag)

    def get_2nd_level_categories(self, response, parent_tag):
        parent_gen_cat = response.meta['gen_cat']
        category_tags = parent_tag.xpath('./div[@class="level-2"]/ul/li')
        if category_tags:
            yield parent_gen_cat
            for index, tag in enumerate(category_tags, 1):
                gen_cat_2 = self._make_category(
                    name=tag.xpath('./a/text()').extract_first().strip(),
                    url=self.make_full_url(tag.xpath('./a/@href').extract_first()),
                    index=index,
                    parent_id=parent_gen_cat['id'],
                )
                response.meta['gen_cat'] = gen_cat_2
                yield from self.get_3rd_level_categories(response, tag)
            return

        yield scrapy.Request(
            url=parent_gen_cat['url'],
            callback=self.get_next_level_categories,
            meta={'gen_cat': parent_gen_cat},
            dont_filter=True,
            errback=self.repeat_request,
        )

    def get_3rd_level_categories(self, response, parent_tag):
        parent_gen_cat = response.meta['gen_cat']
        category_tags = parent_tag.xpath('./ul[@class="level-3 show"]/li')
        if category_tags:
            yield parent_gen_cat
            for index, tag in enumerate(category_tags, 1):
                gen_cat_3 = self._make_category(
                    name=tag.xpath('./a/text()').extract_first().strip(),
                    url=self.make_full_url(tag.xpath('./a/@href').extract_first()),
                    index=index,
                    parent_id=parent_gen_cat['id'],
                )
                yield scrapy.Request(
                    url=gen_cat_3['url'],
                    callback=self.get_next_level_categories,
                    meta={'gen_cat': gen_cat_3},
                    dont_filter=True,
                    errback=self.repeat_request,
                )
            return

        yield scrapy.Request(
            url=parent_gen_cat['url'],
            callback=self.get_next_level_categories,
            meta={'gen_cat': parent_gen_cat},
            dont_filter=True,
            errback=self.repeat_request,
        )

    def get_next_level_categories(self, response):
        parent_gen_cat = response.meta['gen_cat']
        category_tags = self.find_category_tags(response)
        if category_tags:
            yield parent_gen_cat
            for index, tag in enumerate(category_tags, 1):
                next_gen_cat = self._make_category(
                    name=(tag.css('div::text') or tag.css('::text')).extract_first().strip(),
                    url=tag.css('::attr(href)').extract_first(),
                    index=index,
                    parent_id=parent_gen_cat['id'],
                )
                yield scrapy.Request(
                    url=next_gen_cat['url'],
                    callback=self.get_next_level_categories,
                    meta={'gen_cat': next_gen_cat},
                    dont_filter=True,
                    errback=self.repeat_request,
                )
            return

        start_from = 0
        yield scrapy.Request(
            url=self.prepare_products_url(parent_gen_cat['url'], start_from),
            callback=self.parse_products,
            meta={
                'gen_cat': parent_gen_cat,
                'start_from': start_from,
            },
            dont_filter=True,
            errback=self.repeat_request,
        )

    @staticmethod
    def find_category_tags(response):
        category_tags_1 = response.css('#category-level-1 li a')
        active_category_1 = response.css('#category-level-1 > li.expandable.active')
        active_category_2 = response.css('#category-level-1 li.expandable.active.third-category')
        category_tags_2 = response.css('.third-category-content a')
        if category_tags_1 and not active_category_1 and not active_category_2:
            category_tags = category_tags_1
        elif category_tags_2 and not active_category_2:
            category_tags = category_tags_2
        else:
            category_tags = None
        return category_tags

    @staticmethod
    def prepare_products_url(url, start_from):
        return url + GET_PARAMETERS.format(start_from)

    def parse_products(self, response):
        parent_gen_cat = response.meta['gen_cat']
        product_urls = response.xpath('//a[@class="name-link"]/@href').extract()
        for p_url in product_urls:
            color_id_match = re.search(r'color=(\d+)', p_url)
            color_id = color_id_match.group(1) if hasattr(color_id_match, 'group') else ''
            parent_gen_cat['product_urls'].append({
                'url': p_url,
                'id': '_'.join([p_url.split('.htm')[0].split('-')[-1], color_id]),
            })
        if len(product_urls) == 48:
            start_from = response.meta['start_from'] + 48
            yield scrapy.Request(
                url=self.prepare_products_url(response.url.split('?')[0], start_from),
                callback=self.parse_products,
                meta={
                    'gen_cat': parent_gen_cat,
                    'start_from': start_from,
                },
                dont_filter=True,
                errback=self.repeat_request,
            )
            return

        yield parent_gen_cat

    @staticmethod
    def make_full_url(url):
        return url if 'http' in url else BASE_URL.format(url)

    def repeat_request(self, response):
        # Unnecessary meta information
        meta = response.request.meta
        meta_skip_list = ['download_timeout', 'depth', 'proxy', 'download_latency', 'download_slot', ]

        # Retry making request if possible
        iteration = meta.get('iteration', 0) + 1
        if iteration < self.retry_num:
            # Get correct URL (before redirects if were)
            req_url = meta.get("redirect_urls", [response.request.url])[0]

            # Create new Request using old information except those from skip_list
            request = scrapy.Request(
                url=req_url,
                callback=response.request.callback,
                errback=response.request.errback,
                dont_filter=response.request.dont_filter,
                meta={k: v for k, v in meta.items() if k not in meta_skip_list}
            )
            request.meta['iteration'] = iteration
            yield request
            return

        return
