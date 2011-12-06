from datetime import datetime
import time
import re
from BeautifulSoup import BeautifulSoup
from models import *
from crawl import base, train_urls, crawl


def children(tag):
    """Returns the list of children's text"""
    return [x.text for x in tag if hasattr(x, 'text')]


def to_dict(table, headings):
    """Converts HTML table to a list of dicts"""
    d = []
    rows = []

    body_list = table.findChildren('tbody', recursive=False)
    if not body_list:
        # There is no tbody
        body_list = [table]

    for body in body_list:
        rows = rows + body.findChildren('tr', recursive=False)

    for row in rows:
        cells = children(row)
        if len(cells) == 1:
            continue
        d.append(dict(zip(headings, cells)))
    return d


def clean_keys(items):
    """Renames the keys in items list to confirm to db fields"""
    m =  { 'no.' : 'number',
           'date from' : 'date_from',
           'date to' : 'date_to',
           'from' : 'from_station',
           'to' : 'to_station',
           'dep days' : 'days',
           '#' : 'stop_number',
           'station name' : 'station_name',
           'day#' : 'day',
         }

    for item in items:
        for key,value in item.items():
            lkey = key.lower()
            new_key = m.get(lkey) or lkey
            item[new_key] = value
            del item[key]
    return items



def get_train_links(table):
    # Links to train have css class "detailsprominent" and title is set. Links to station have the same class, but no title.
    urls = table.findAll('a', 'detailsprominent', title=True)
    links = {}
    for url in urls:
        href = base + url['href']
        assert '/train/' in href
        text = url.text
        number = text.split('/')[-1]
        name = text.split('/')[0]
        links[number] = (name, href)

    return links


def update_departure_days(table):
    """Required to put 0 when the train does not run a day. Otherwise can not know what T or S stand for"""
    grids = table.findAll('table', 'deparrgrid')
    for grid in grids:
        for cell in grid.findAll('td'):
            if cell.text not in ['S', 'M', 'T', 'W', 'F']:
                cell.replaceWith('0')


def extract_trains():
    """Extracts the train data from html dump"""
    db.query(TempTrain).delete() # Truncate TempTrain
    db.commit()

    for row in db.query(Raw.url):
        url = row.url
        if url not in train_urls:
            continue

        debug("Processing html of %s" % url)
        raw_html = db.query(Raw.html).filter_by(url=url).first()
        s = BeautifulSoup(raw_html.html)
        table = s.find('table', id='SearchResultsTable')
        update_departure_days(table)

        header = table.find('tr', 'tableheader').extract()
        headings = [td.text for td in header]

        trains = to_dict(table, headings)
        trains = clean_keys(trains)

        train_links = get_train_links(table)
        s.decompose()

        for train in trains:
            number = train['number']
            train['name'] = train_links[number][0]
            train['url'] = train_links[number][1]

            db.add(TempTrain(**train))
            db.commit()


def update_return_train(train, s):
    ret = s.find(text=re.compile("Return Journey"))
    if not ret or not hasattr(ret.nextSibling, 'text'):
        return
    return_train = ret.nextSibling.text
    #assert return_train.startswith('Train#')
    train.return_train = return_train.replace('Train#', '').strip()


def extract_train_schedules():
    """Extracts the train schedule data from html dump"""
    db.query(TempSchedule).delete() # Truncate TempSchedule
    db.commit()

    count = db.query(func.count(Raw.id)).scalar()
    for i, row in enumerate(db.query(Raw.url)):
        url = row.url
        if url in train_urls:
            continue

        debug("Processing html of %s (%s of %s. Remaining %s)" % (url, i, count, count-i))
        train = db.query(TempTrain).filter_by(url=url).first()
        raw_html = db.query(Raw.html).filter_by(url=url).first()
        raw_html = raw_html.html
        s = BeautifulSoup(raw_html)

        assert train.number in s.text
        #assert unicode(train.name, 'utf-8') in s.text

        table = s.find('table', 'schtable')
        header = table.find('tr', 'first-child').extract()
        headings = [td.text for td in header]

        schedule = to_dict(table, headings)
        schedule = clean_keys(schedule)


        for sch in schedule:
            sch['train_number'] = train.number
            db.add(TempSchedule(**sch))

        update_return_train(train, s)
        s.decompose()
        db.commit()


def crawl_train_schedules():
    debug("Crawling Train schedules")
    count = db.query(func.count(TempTrain.id)).scalar()
    for i, row in enumerate(db.query(TempTrain.url)):
        debug("Crawing %s of %s. Remaining %s" % (i, count, count-i))
        crawl(row.url)


def main():
    extract_trains()
    crawl_train_schedules()
    extract_train_schedules()


if __name__ == '__main__':
    main()
