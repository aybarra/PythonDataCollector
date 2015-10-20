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

class DmozSpider(scrapy.Spider):
    name = "dmoz"
    allowed_domains = ["pro-football-reference.com"]
    start_urls = [
        "http://www.pro-football-reference.com/players/qbindex.htm"
    ]

    print scrapy.settings.default_settings
    download_delay = 0.50

    def __init__(self, category=None, *args, **kwargs):
        super(DmozSpider, self).__init__(*args, **kwargs)
        # If they had college stats
        self.gameLogsXPathCollege = "//*/*[1]/*/*[3]/a[contains(.,'Gamelogs')]"

        # No college stats
        self.gameLogsXPathNoCollege = "//*/*[1]/*/*[2]/a[contains(.,'Gamelogs')]"

        # Initialize a new Firefox webdriver
        profile = FirefoxProfile("/Users/andrasta/Desktop/InfoVis/project/FirefoxProfileUpdated")
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', os.getcwd()+"/QB")
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
        for letter in range(1,2):#len(letter_count)+1):
            # Gets us to a particular letter
#            xpath_inactive = response.xpath("//*/blockquote[" + str(letter) + "]/pre/a")
##            print "Number of inactive players for letter "+ str(letter) +" is: "+ str(len(xpath_inactive))
            string_letter = response.xpath("//*/blockquote["+ str(letter) + "]/preceding-sibling::*[1]/text()")[0].extract()
            self.logger.info("String letter is: %s", string_letter)
            xpath_active = response.xpath("//*/blockquote[" + str(letter) + "]/pre/b")
            self.logger.info("Number of active players for letter %s is: %s", string_letter, str(len(xpath_active)))

#            # Inactive players
##            for player_index in range(1,len(xpath_inactive)):
##                player_link = response.xpath("//*/blockquote[" + str(letter) + "]/pre/a["+ str(player_index) + "]/@href")
###                print player_link
##                if len(player_link) > 0:
##                    url = response.urljoin(player_link[0].extract())
##                    yield scrapy.Request(url, callback=self.navGameLog)
#
#
            # Active players
            for player_index in range(1,len(xpath_active)+1):
                player_link = response.xpath("//*/blockquote[" + str(letter)
                                             + "]/pre/b["+ str(player_index) + "]/a/@href")
#                print len(player_link)
                # Checking if the link exists
                if len(player_link) > 0:
                    url = response.urljoin(player_link[0].extract())
#                    print url
                    yield scrapy.Request(url, callback=self.navGameLog)

    def navGameLog(self, response):
        # Finding players name
        player_name = response.xpath("//*[@id=\"you_are_here\"]/p/span[5]/b/span/text()")[0].extract()

        # Find player's pfr name
        pfr_name = response.url.rsplit('/',1)[-1]
        htm_index = pfr_name.find(".htm")
        pfr_name = pfr_name[0:htm_index]

        self.logger.info("Player's name is: %s", player_name)
        self.logger.info("Player's pfr_name is: %s", pfr_name)

#        with open("qb_names.txt", "a") as outfile:
#            json.dump({'pro_football_ref_name':pfr_name, 'player_full_name':player_name}, outfile, indent=4)
#            outfile.write("\n")

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
            return

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


