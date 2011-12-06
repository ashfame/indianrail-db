import urllib2
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from models import *

base = 'http://indiarailinfo.com/'
train_urls = 'new passenger emudmu toy slip rajdhani shatabdi duronto janshatabdi garibrath samparkkranti mailexpress sf special hyderabad delhiparikrama'
train_urls = [base + 'trains/' + url for url in train_urls.split()]

css_selector = '.topcapsule table#SearchResultsTable~div button'


def sleep(mins):
    debug("Sleeping for %s minutes" % mins)
    time.sleep(mins * 60)

def get_html_with_selenium(url):
    debug("Opening %s" % url)
    browser = webdriver.Firefox()
    browser.get(url)
    assert "India Rail Info" in browser.title

    while True:
        try:
            sleep(1)
            #scroll to bottom
            browser.execute_script("window.scrollTo(0, 1000000);")
            #click on next page
            button = browser.find_element_by_css_selector(css_selector)
            debug("Clicking on next page")
            button.click()
        except NoSuchElementException:
            debug("No more pages")
            break

    html = browser.page_source
    browser.close()
    return html


def get_html(url):
    """Returns html content of the url. Retries until successful without overloading the server."""
    while True:
        # Retry until succesful
        try:
            sleep(2)
            debug('Crawling %s' % url)
            html = urllib2.urlopen(url).read()
            return html
        except urllib2.HTTPError, e:
            warn('HTTP error %s while crawling %s. Trying again.' % (e, url))
            sleep(5)
            continue
        except urllib2.URLError, e:
            warn('URL error %s while crawling %s. Trying again.' % (e, url))
            sleep(5)
            continue


def crawl_trains():
    """Crawls the urls specified in train_urls."""
    for url in train_urls:
        # We need to use selenium because indianrailinfo.com uses lot of ajax.
        html = get_html_with_selenium(url)
        db.add(Raw(url, html))
        db.commit()


def crawl(url):
    """Crawls a single url"""
    if exists(Raw, url=url):
        debug("%s is already crawled" % url)
    else:
        html = get_html(url)
        db.add(Raw(url, html))
        db.commit()
