import scrapy
from scrapy import signals
from scrapy.exceptions import NotConfigured, CloseSpider
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.webdriver import FirefoxProfile
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Only so we can call our own api running on localhost
import requests
from requests.auth import HTTPBasicAuth

import json
import sys
from tutorial.items import DmozItem

class DmozSpider(scrapy.Spider):
    name = "dmoz"
    allowed_domains = ["pro-football-reference.com"]
    start_urls = [
        "http://www.pro-football-reference.com/players/qbindex.htm"
#        "http://www.pro-football-reference.com/players/rbindex.htm"
#        "http://www.pro-football-reference.com/players/wrindex.htm"
#        "http://www.pro-football-reference.com/players/teindex.htm"
    ]

    print scrapy.settings.default_settings
    download_delay = 0.75

    def __init__(self, category=None, *args, **kwargs):
        super(DmozSpider, self).__init__(*args, **kwargs)

        self.selection = [kwargs.get('selection')][0]
        self.logger.info("Selection is: %s", str(self.selection))
        if self.selection != 'active' and self.selection != 'retired':
            sys.exit("Invalid selection")
        elif self.selection == None:
            sys.exit("Selection not set")

        self.output_dir = [kwargs.get('output_dir')][0]
        self.logger.info("Output directory is: %s", str(self.output_dir))
        if self.output_dir == None:
            sys.exit("No output directory specified")

        self.position_type = [kwargs.get('position_type')][0]
        self.logger.info("Position type is: %s", str(self.position_type))
        if self.position_type != 'qb' and self.position_type != 'wr' and self.position_type != 'rb' and self.position_type != 'te':
            sys.exit("Invalid position type")

        collect_csvs = [kwargs.get('collect_csvs')][0]
        self.logger.info("Collect csvs is set to: %s", str(collect_csvs))
        if collect_csvs == None:
            sys.exit("Collect csvs is not set!")
        elif collect_csvs == "True":
            self.collect_csvs = True
        elif collect_csvs == "False":
            self.collect_csvs = False
        else:
            sys.exit("Collect csvs set to invalid value")

        if self.selection == 'retired':
            self.old_guys = [kwargs.get('old_guys')][0]
            self.logger.info("Old Guys is set to %s", str(self.old_guys))
            if self.old_guys == None:
                sys.exit("Old guys not set!")
            elif self.old_guys != "True" and self.old_guys != "False":
                sys.exit("Old guys set to invalid value!")


        # If they had college stats
        self.gameLogsXPathCollege = "//*/*[1]/*/*[3]/a[contains(.,'Gamelogs')]"

        # No college stats
        self.gameLogsXPathNoCollege = "//*/*[1]/*/*[2]/a[contains(.,'Gamelogs')]"

        # Initialize a new Firefox webdriver
        profile = FirefoxProfile("/Users/andrasta/Desktop/InfoVis/project/FirefoxProfileUpdated")
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', os.getcwd()+"/"+self.output_dir) #os.getcwd()+"/QB")
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
        profile.set_preference('browser.helperApps.neverAsk.openFile', 'text/csv')

        self.driver = webdriver.Firefox(firefox_profile=profile)
        # Doesn't matter
#        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 5)

        self.no_stat_players = []


    def parse(self, response):
        self.logger.info("Visiting url: %s", response.url)
        # Get the players link

#        print response.body
        letter_count = response.xpath('//*/blockquote')
        self.logger.info("The number of QB letters is: %i", len(letter_count))

        # For each of the letter blocks
        for letter in range(1, len(letter_count)+1):
            # Gets us to a particular letter
            xpath_inactive = response.xpath("//*/blockquote[" + str(letter) + "]/pre/a")
##            print "Number of inactive players for letter "+ str(letter) +" is: "+ str(len(xpath_inactive))
            string_letter = response.xpath("//*/blockquote["+ str(letter) + "]/preceding-sibling::*[1]/text()")[0].extract()
            self.logger.info("String letter is: %s", string_letter)
            xpath_active = response.xpath("//*/blockquote[" + str(letter) + "]/pre/b")


            # Inactive players
            if self.selection == 'retired':
                self.logger.info("NUMBER OF PLAYERS WITH LETTER %i is: %i", letter, len(xpath_inactive)+1)
                for player_index in range(1, len(xpath_inactive)+1):
                    player_link = response.xpath("//*/blockquote[" + str(letter)
                                                 + "]/pre/a["+ str(player_index) + "]/@href")
    #                print player_link
                    played_text = response.xpath("normalize-space(//*/blockquote[" + str(letter)
                                                 + "]/pre/a["+ str(player_index)
                                                 + "]/following-sibling::text()[1])").extract_first()

                    self.logger.info("played_text is %s", played_text)
                    # Need to get the last occurrence because of mixed qb types
                    dash_index = played_text.rfind("-")
                    self.logger.info("Dash index is %s", dash_index)
                    if dash_index != -1:
                        end_year = int(played_text[dash_index+1:])
                        start_year = int(played_text[dash_index-4:dash_index])
                        self.logger.info("start_date and end_date is: %i, %i", start_year, end_year)

                        if self.old_guys:
                            if len(player_link) > 0:
                                # Only fetch old guys
                                if start_year <= 1960:
                                    url = response.urljoin(player_link[0].extract())
                                    request = scrapy.Request(url, callback=self.navSeasonCSV)
                                    request.meta['start_year'] = start_year
                                    request.meta['end_year'] = end_year
                                    yield request
                        else:
                            if len(player_link) > 0:
                                url = response.urljoin(player_link[0].extract())
                                request = scrapy.Request(url, callback=self.navGameLog)
                                request.meta['start_year'] = start_year
                                request.meta['end_year'] = end_year
                                yield request

            # Active players
            if self.selection == 'active':
                self.logger.info("Number of active players for letter %s is: %s", string_letter, str(len(xpath_active)))
                for player_index in range(1, len(xpath_active)+1):
                    player_link = response.xpath("//*/blockquote[" + str(letter)
                                                 + "]/pre/b["+ str(player_index) + "]/a/@href")
                    played_text = response.xpath("normalize-space(//*/blockquote[" + str(letter)
                                                 + "]/pre/b["+ str(player_index)
                                                 + "]/a/following-sibling::text()[1])").extract_first()
                    dash_index = played_text.rfind("-")
                    if dash_index != -1:
                        end_year = int(played_text[dash_index+1:])
                        start_year = int(played_text[dash_index-4:dash_index])

                        self.logger.info("start_date and end_date is: %i, %i", start_year, end_year)

                        # Checking if the link exists
                        if len(player_link) > 0:
                            url = response.urljoin(player_link[0].extract())
                            request = scrapy.Request(url, callback=self.navGameLog)
                            request.meta['start_year'] = start_year
                            request.meta['end_year'] = end_year
                            yield request

    def navSeasonCSV(self, response):
        item = DmozItem()

        # Find player's name
        player_name = response.xpath("//*[@id=\"you_are_here\"]/p/span[5]/b/span/text()")[0].extract()
        item['player_name'] = player_name
        item['position_type'] = self.position_type

        # Find player's pfr name
        pfr_name = response.url.rsplit('/',1)[-1]
        htm_index = pfr_name.find(".htm")
        pfr_name = pfr_name[0:htm_index]
        item['pfr_name'] = pfr_name
        item['start_year'] = response.meta['start_year']
        item['end_year'] = response.meta['end_year']

        if self.collect_csvs == True:

            # Load the response url in selenium
            self.driver.get(response.url)
            self.logger.debug("Selenium navigated to %s", str(response.url))

            season_passingCSVXPath = "(//h2[contains(.,'Passing')]/following-sibling::*[1]/*[contains(.,'Export')])[1]"

            # Find the passing export link
            csvLink = self.wait.until(EC.element_to_be_clickable((By.XPATH, season_passingCSVXPath)))
            if csvLink != None: self.logger.info("Text for the export link is: %s", csvLink.text)
            if csvLink != None and csvLink.text == "Export":
                ActionChains(self.driver).click(csvLink).perform()
                self.logger.debug("Selenium clicked the csv export link")
            else:
                self.logger.error("Player %s has no passing stats to export", player_name)


            season_rushingCSVXPath = "(//h2[contains(.,'Rushing & Receiving')]/following-sibling::*[1]/*[contains(.,'Export')])[1]"
            csvLink = self.wait.until(EC.element_to_be_clickable((By.XPATH, season_rushingCSVXPath)))
            if csvLink != None: self.logger.info("Text for the export link is: %s", csvLink.text)
            if csvLink != None and csvLink.text == "Export":
                ActionChains(self.driver).click(csvLink).perform()
                self.logger.debug("Selenium clicked the csv export link")
            else:
                self.logger.error("Player %s has no rushing&receiving stats to export", player_name)
        yield item

    def navGameLog(self, response):

        item = DmozItem()
        # Find player's name
        player_name = response.xpath("//*[@id=\"you_are_here\"]/p/span[5]/b/span/text()")[0].extract()
        item['player_name'] = player_name
        item['position_type'] = self.position_type

        # Find player's pfr name
        pfr_name = response.url.rsplit('/',1)[-1]
        htm_index = pfr_name.find(".htm")
        pfr_name = pfr_name[0:htm_index]
        item['pfr_name'] = pfr_name
        item['start_year'] = response.meta['start_year']
        item['end_year'] = response.meta['end_year']

        draft_year = -1
        # Find the year the player was drafted, and override the start_year value if its earlier
        if len(response.xpath("//strong[contains(.,'Drafted')]")) > 0 and response.xpath("//strong[contains(.,'Drafted')]")[0].extract() != None:
            if len(response.xpath("//a[contains(.,'NFL Draft')]/text()")) > 0:
                draft_year = response.xpath("//a[contains(.,'NFL Draft')]/text()")[0].extract()
                nfl_index = draft_year.find("NFL Draft")
                if nfl_index != -1:
                    draft_year = draft_year[0:nfl_index-1]
                    if draft_year != None and int(draft_year) < item['start_year']:
                        item['start_year'] = int(draft_year)
            elif len(response.xpath("//a[contains(.,'NFL Supplemental Draft')]/text()")) > 0:
                draft_year = response.xpath("//a[contains(.,'NFL Supplemental Draft')]/text()")[0].extract()
                nfl_index = draft_year.find("NFL Supplemental Draft")
                if nfl_index != -1:
                    draft_year = draft_year[0:nfl_index-1]
                    if draft_year != None and int(draft_year) < item['start_year']:
                        item['start_year'] = int(draft_year)

        self.logger.info("Player's name is: %s, pfr_name is: %s, draft_year is: %s", player_name, pfr_name, draft_year)

        # Checking to see if gamelogs exist
        gameLogsXPath = ""
        if len(response.xpath(self.gameLogsXPathCollege)) != 0:
            gameLogsXPath = self.gameLogsXPathCollege
            self.logger.info("%s has college stats", player_name)

        elif len(response.xpath(self.gameLogsXPathNoCollege)) != 0:
            gameLogsXPath = self.gameLogsXPathNoCollege
            self.logger.info("%s has no college stats", player_name)

        else:
            self.logger.error("No game logs link for %s", player_name)
            # Even if they don't have stats we need their name for record keeping
#            yield item

        # Will grab the regular csvs
        if self.collect_csvs == True:
            if gameLogsXPath != "":

                # Load the response url in selenium
                self.driver.get(response.url)
                self.logger.debug("Selenium navigated to %s", str(response.url))

                # Click the game logs link
                gameLogsLink = self.wait.until(EC.element_to_be_clickable((By.XPATH, gameLogsXPath)))

                ActionChains(self.driver).click(gameLogsLink).perform()
                self.logger.debug("Selenium clicked game logs link")

                # Find the export link
    #            csvXPath = "//*/div[3]/div[1]/div/*[contains(.,'Export')]"
                csvXPath = "(//span[contains(.,'Export')])[1]"
    #            csvXPath = "/html/body/div[1]/div[4]/div[3]/div[1]/div/*[contains(.,'Export')]"

                csvLink = self.wait.until(EC.element_to_be_clickable((By.XPATH, csvXPath)))
                self.logger.info("Text for the export link is: %s", csvLink.text)
                if csvLink != None and csvLink.text == "Export":
    #                ActionChains(self.driver).click(csvLink).perform()
    #                ActionChains(self.driver).move_to_element(csvLink).perform()
                    ActionChains(self.driver).click(csvLink).perform()
                    self.logger.debug("Selenium clicked the csv export link")
                else:
                    self.logger.error("Player %s has game logs link, but nothing to export", player_name)
            else:
                self.logger.error("Player %s falls into a different case than expecting", player_name)

        yield item


