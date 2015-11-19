# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DmozItem(scrapy.Item):
    player_name = scrapy.Field()
#    draft_year = scrapy.Field()
    pfr_name = scrapy.Field()
    position_type = scrapy.Field()
    start_year = scrapy.Field()
    end_year = scrapy.Field()
