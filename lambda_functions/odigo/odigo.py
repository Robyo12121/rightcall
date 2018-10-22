#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from requestium import Session
from selenium.webdriver.common.action_chains import ActionChains
from dotenv import load_dotenv

load_dotenv()
username = os.environ.get('PROSODIE_USERNAME')
passwd = os.environ.get('PROSODIE_PASSWORD')
print(sys.version)


def change_date_format(date):
    try:
        correct_string = date.strptime(str(date.date()), '%Y-%m-%d').strftime('%m-%d-%Y')
        return correct_string
    except Exception as e:
        raise e


def change_time_format(date):
    try:
        correct_string = date.strptime(str(date.hour) + ':' + str(date.minute), "%H:%M").strftime("%I:%M %p")
        if correct_string[0] == "0":
            return correct_string[1::]
        else:
            return correct_string
    except Exception as e:
        raise e


def set_range(now):
    """
    Takes current datetime and finds the nearest, previous half hour.
    Returns the appropriate start and end times and date
    """
    # Format: '10-19-2018'
    # Format: '12:00 AM'

    def ceil_dt(dt, delta):
        """Round up to the nearest half hour"""
        return dt + (datetime.datetime.min - dt) % delta

    hour_ago = now - datetime.timedelta(minutes=60)
    rounded = ceil_dt(hour_ago, datetime.timedelta(minutes=30))

    start_date = change_date_format(rounded)
    start_time = change_time_format(rounded)
    thirty_mins = datetime.timedelta(minutes=30)
    end_date = start_date
    end_time = change_time_format(rounded + thirty_mins)
    return (start_date, start_time, end_date, end_time)


def setup():
    driver = r'C:\Users\RSTAUNTO\Desktop\chromedriver.exe'
    s = Session(webdriver_path=driver,
                browser='chrome',
                default_timeout=15,
                webdriver_options={'arguments': ['headless']})
    return s


def download_mp3(s, path=None, ref=None):
    """Download mp3 file from www.prosodie.com page and return session.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        path -- mp3 file absolute path (not required | type: str);
        ref -- ref number (not required | type: str).
        Example: '3905beTOd10339';
    Output:
        s -- Requestium session (type: requestium.requestium.Session).

    """

    s.driver.ensure_element_by_class_name('x-action-col-icon').click()
    s.driver.switch_to.frame('result_frame')
    time.sleep(1)
    # Get URL of mp3 file
    src = s.driver.ensure_element_by_id('messagePlayer').get_attribute('src')
    # Selenium --> Requests
    s.transfer_driver_cookies_to_session()
    # Download
    r = s.get(src, stream=True)
    if path is None:
        if ref is None:
            # Get ref number
            soap = BeautifulSoup(s.driver.page_source, 'lxml')
            ref = soap.findAll('div', class_='x-grid-cell-inner')[1].text
        path = '%s.mp3' % ref
    if r.status_code == 200:
        with open(path, 'wb') as f:
            for chunk in r.iter_content(1024*2014):
                f.write(chunk)
    else:
        return 1
    # Requests --> Selenium
    s.transfer_session_cookies_to_driver()
    return s


def download_mp3_by_ref(s, username, passwd, ref, path=None):
    """Download mp3 file from www.prosodie.com page by ref number.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        username -- username on www.prosodie.com (required | type: str);
        passwd -- password for username on www.prosodie.com (required |
                  type: str);
        ref -- ref number (required | type: str). Example: '3905beTOd10339';
        path -- mp3 file absolute path (not required | type: str).

    """

    s = login(s, username, passwd)
    s = search_by_ref(s, ref)
    result = download_mp3(s, path, ref)
    if result == 1:
        return 1
    s.driver.close()


def download_mp3_by_csv(s, username, passwd, csv_path, download_dir=None):
    """Download mp3 file/files from www.prosodie.com page by input csv file.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        username -- username on www.prosodie.com (required | type: str);
        passwd -- password for username on www.prosodie.com (required |
                  type: str);
        csv_path -- csv file absolute path (required | type: str);
        download_dir -- download directory for mp3 file/files (not required |
                  type: str).

    """

    s = login(s, username, passwd)
    refs = pd.read_csv(csv_path, sep=';').Name
    length = len(refs)
    for i, ref in enumerate(refs):
        sys.stdout.write('\r')
        sys.stdout.write('downloading: %s/%s' % (i+1, length))
        sys.stdout.flush()
        s = search_by_ref(s, ref)
        mp3_path = None
        if download_dir is not None:
            file_name = '%s.mp3' % ref
            mp3_path = os.path.join(download_dir, file_name)
        result = download_mp3(s, mp3_path, ref)
        if result == 1:
            return 1
    sys.stdout.write('\n')
    sys.stdout.flush()
    s.driver.close()


def download_all_csv(s, username, passwd, download_dir=None):

    s = setup()
    d = datetime.datetime.now()
    s = login(s, username, passwd)
    search_range = set_range(d)
    print(search_range[0], search_range[1], search_range[2], search_range[3])
    s = search_by_range(s, search_range[0],
                        search_range[1],
                        search_range[2],
                        search_range[3])
    s = search_by_language(s, language="_EN")
    # s.driver.execute_script("zipItems();")
    csvB = s.driver.ensure_element_by_id('csvButton')
    if csvB.is_displayed():
        print("csvB is visible")
        csvB.ensure_click()
    else:
        print("Not Visible")
    yes = s.driver.ensure_element_by_id("button-1006")
    # yes = s.driver.ensure_element_by_css_selector("#button-1006")
    # yes.ensure_click()
    # full_xpath = """//div[@id='messagebox-1001']/div[@id='messagebox-1001-toolbar']/div[@id='messagebox-1001-toolbar-innerCt']/div[@id='messagebox-1001-toolbar-targetEl']/a[@id='button-1006'])"""
    # xpath_messagebox = "//div[@id='messagebox-1001']"
    # css_sel_messagebox = '.x-css-shadow'
    # yes = s.driver.ensure_element_by_css_selector(css_sel_messagebox)
    # if yes.is_displayed():
    #     print("Yes is visible")
    #     yes.ensure_click()
    # else:
    #     print("Yes button not visible")
    # s.driver.ensure_element_by_id('button-1006').ensure_click()
    s.driver.close()
    # return element


def check_num_results(s):
    url = 'https://enregistreur.prosodie.com/odigo4isRecorder/' \
          'EntryPoint?serviceName=CriteresMessagesHandler&lang=en'
    s.driver.get(url)
    result = s.driver.ensure_element_by_id('resultLabelId').get_attribute("innerText")
    return result


def login(s, username, passwd):
    """Login to www.prosodie.com with username/passwd pair and return session.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        username -- username on www.prosodie.com (required | type: str);
        passwd -- password for username on www.prosodie.com (required |
                  type: str).
    Output:
        s -- Requestium session (type: requestium.requestium.Session).

    """

    url = 'https://enregistreur.prosodie.com/odigo4isRecorder/' \
          'EntryPoint?serviceName=LoginHandler'
    s.driver.get(url)
    s.driver.ensure_element_by_name('mail').send_keys(username)
    s.driver.ensure_element_by_name('password').send_keys(passwd)
    s.driver.ensure_element_by_name('valider').click()
    return s


def search_by_range(s, start_date=None, start_time=None, end_date=None,
                    end_time=None):
    """Search records on www.prosodie.com by date range and return session.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        start_date -- start date (not required | type: str). Format:
                      'mm:dd:yyyy'. Example: '03-05-1991';
        start_time -- start time (not required | type: str). Example:
                      '12:00 AM';
        end_date -- end date (not required | type: str). Format:
                    'mm:dd:yyyy'. Example: '03-05-1991';
        end_time -- end time (not required | type: str). Example: '12:00 PM'.
    Output:
        s -- Requestium session (type: requestium.requestium.Session).

    """

    url = 'https://enregistreur.prosodie.com/odigo4isRecorder/' \
          'EntryPoint?serviceName=CriteresMessagesHandler&lang=en'
    s.driver.get(url)
    if start_date:
        s.driver.ensure_element_by_name('dateDebut').send_keys(start_date)
    if start_time:
        s.driver.ensure_element_by_name('heureDebut').send_keys(start_time)
    if end_date:
        s.driver.ensure_element_by_name('dateFin').send_keys(end_date)
    if end_time:
        s.driver.ensure_element_by_name('heureFin').send_keys(end_time)
    s.driver.ensure_element_by_id('button-1009').click()
    return s


def search_by_language(s, language=None):
    """
    Filter results by language
    Options: "_EN" : English, "_ES" : Spanish etc.
    """
    url = 'https://enregistreur.prosodie.com/odigo4isRecorder/' \
          'EntryPoint?serviceName=CriteresMessagesHandler&lang=en'
    s.driver.get(url)
    if language:
        s.driver.ensure_element_by_name('criteres').send_keys(language)
    s.driver.ensure_element_by_id('button-1009').click()
    return s


def search_by_ref(s, ref):
    """Search record on www.prosodie.com by ref number and return session.
    Input:
        s -- Requestium session (required |
             type: requestium.requestium.Session);
        ref -- ref number (required | type: str). Example: '3905beTOd10339'.
    Output:
        s -- Requestium session (type: requestium.requestium.Session).

    """

    url = 'https://enregistreur.prosodie.com/odigo4isRecorder/' \
          'EntryPoint?serviceName=CriteresMessagesHandler&lang=en'
    s.driver.get(url)
    s.driver.ensure_element_by_name('refEr').send_keys(ref)
    s.driver.ensure_element_by_id('button-1009').click()
    return s


if __name__ == '__main__':
    s = setup()
    element = download_all_csv(s, username, passwd)
    # print(element)
    # print(type(element))
    # download_mp3_by_csv(s, username, passwd, )
    # download_mp3_by_ref(s, username, passwd, 'bda551TVd00927',
    # r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\lambda_functions\myMP3.mp3')
    # download_mp3_by_ref(s, username, passwd, 'b76993TOd10547')
    # download_mp3_by_csv(s, username, passwd,
    #                     'csvs/toget.csv', download_dir='mp3s')
    # Example. Download mp3 file from www.prosodie.com by '3905beTOd10339'
    # ref number
    # download_mp3_by_ref(s, username, passwd, '3905beTOd10339')

    # Example. Download mp3 file from www.prosodie.com by '3905beTOd10339'
    # ref number as /tmp/example.mp3
    # download_mp3_by_ref(s, username, passwd,
    # '3905beTOd10339', '/tmp/example.mp3')

    # Example. Download mp3 file/files from www.prosodie.com
    # page by input csv file
    # download_mp3_by_csv(s, username, passwd, 'input.csv')

    # Example. Download mp3 file/files from www.prosodie.com page
    # by input csv file
    # to dedicated directory
    # download_mp3_by_csv(s, username, passwd, 'input.csv', download_dir='/tmp')
