import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.webdriver import FirefoxProfile
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def run_test():
    profile = FirefoxProfile("/Users/andrasta/Desktop/InfoVis/project/FirefoxProfile")
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.dir', os.getcwd()+"/QB")
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    profile.set_preference('browser.helperApps.neverAsk.openFile', 'text/csv')

    driver = webdriver.Firefox(firefox_profile=profile)
    # Doesn't matter
#        self.driver.maximize_window()
    wait = WebDriverWait(driver, 5)
    driver.maximize_window()
    driver.get("http://www.pro-football-reference.com/players/B/BreeDr00/gamelog//")

    csvXPath = "(//span[contains(.,'Export')])[1]"

    csvLink = wait.until(EC.element_to_be_clickable((By.XPATH, csvXPath)))
    print "XPATH found link text is: ", csvLink.text
    if csvLink != None and csvLink.text == "Export":
#        ActionChains(driver).move_to_element(csvLink).perform()
        ActionChains(driver).click(csvLink).perform()

run_test()
