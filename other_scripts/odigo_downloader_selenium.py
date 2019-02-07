import logging
from selenium import webdriver
from requestium import Session
import time
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os
import datetime


class CustomException(Exception):
    pass


class Downloader():

    def __init__(
            self,
            username,
            password,
            driver_path=None,
            download_path=None,
            headless=True,
            logger=None
    ):
        if not logger:
            logging.basicConfig(level=logging.DEBUG)
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel('DEBUG')
        else:
            self.logger = logger
        self._username = username
        self._password = password
        self.driver_path = driver_path
        self.download_path = download_path
        self.logger = logging.getLogger('odigo_downloader.downloader')
        self.url = 'https://enregistreur.prosodie.com/odigo4isRecorder/EntryPoint?serviceName=LoginHandler'
        self.headless = headless
        self.validated = False
        self.active = False

    def __str__(self):
        return f"\nDOWNLOAD PATH: {self.download_path}\nOPTIONS: {self.webdriver_options}\n" \
            f"DRIVER PATH: {self.driver_path}\nUSERNAME: {self._username}\nURL: {self.url}"

    def setup_selenium_browser(self):
        if self.active:
            return f"Session/Browser already active. Cannot have two concurrent sessions/browsers"
        options = webdriver.ChromeOptions()
        prefs = {
            'download.default_directory': self.download_path,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': False,
            'safebrowsing.disable_download_protection': True}
        options.add_experimental_option('prefs', prefs)

        if self.headless:
            options.add_argument('--headless')

        self.browser = webdriver.Chrome(self.driver_path, options=options)

        if self.headless:
            self.browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
            params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': self.download_path}}
            command_result = self.browser.execute("send_command", params)
            for key in command_result:
                self.logger.debug("result:" + key + ":" + str(command_result[key]))

        self.active = True

    def setup_requestium_session(self):
        if self.active:
            return f"Session/Browser already active. Cannot have two concurrent sessions/browsers"      
        if self.headless:
            webdriver_options = {'arguments': ['headless']}
        else:
            webdriver_options = {}
        self.logger.debug(f"Creating Session object with values: {webdriver_options}")
        self.session = Session(
            webdriver_path=self.driver_path,
            browser='chrome',
            default_timeout=15,
            webdriver_options=webdriver_options)
        self.active = True

    def login_requestium(self):
        if self.active:
            raise CustomException(f"Cannot have two active sessions/browsers")
        self.setup_requestium_session()
        self.logger.debug(f"Going to URL: {self.url}")
        self.session.driver.get(self.url)
        self.logger.debug(f"Entering credentials")
        self.session.driver.ensure_element_by_name('mail').send_keys(self._username)
        self.session.driver.ensure_element_by_name('password').send_keys(self._password)
        self.session.driver.ensure_element_by_name('valider').click()
        self.validated = True

    def login_selenium(self):
        if self.active:
            raise CustomException(f"Cannot have two active sessions/browsers")
        self.setup_selenium_browser() 
        self.browser.get(self.url)
        self.browser.find_element_by_name('mail').send_keys(username)
        self.browser.find_element_by_name('password').send_keys(password)
        self.browser.find_element_by_name('valider').click()
        return

    def download_mp3(self, path=None, ref=None, xpath=None):
        self.logger.info(f"\ndownload_mp3 called with:\nPATH: {path},\nREF: {ref},\nXPATH: {xpath}")
        if ref is not None and xpath is None:
            self.session.driver.ensure_element_by_class_name('x-action-col-icon').click()
        elif xpath is not None and ref is None:
            self.session.driver.ensure_element_by_xpath(xpath).click()
        else:
            self.logger.error("Cannot use both reference number and xpath")
            return
        self.session.driver.switch_to.frame('result_frame')
        time.sleep(1)
        # Get URL of mp3 file
        src = self.session.driver.ensure_element_by_id('messagePlayer').get_attribute('src')
        # Selenium --> Requests
        self.session.transfer_driver_cookies_to_session()
        # Download
        r = self.session.get(src, stream=True)
        if path is None:
            if ref is None:
                # Get ref number
                soap = BeautifulSoup(self.session.driver.page_source, 'lxml')
                ref = soap.findAll('div', class_='x-grid-cell-inner')[1].text
            path = '%s.mp3' % ref
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r.iter_content(1024 * 2014):
                    f.write(chunk)
        else:
            return 1
        # Requests --> Selenium
        self.session.transfer_session_cookies_to_driver()
        self.session.driver.switch_to.default_content()
        return

    def download_mp3_by_ref(self, ref, path=None):
        self.login_requestium()
        self.search_by_ref(ref)
        result = self.download_mp3(path, ref)
        if result == 1:
            return 1
        self.session.driver.close()

    def download_mp3_by_csv(self, csv_path, download_dir=None):
        if download_dir is None:
            download_dir = self.download_path
        self.login_requestium()
        refs = pd.read_csv(csv_path, sep=';').Name
        length = len(refs)
        for i, ref in enumerate(refs):
            sys.stdout.write('\r')
            sys.stdout.write('downloading: %s/%s' % (i + 1, length))
            sys.stdout.flush()
            self.search_by_ref(ref)
            mp3_path = None
            if download_dir is not None:
                file_name = '%s.mp3' % ref
                mp3_path = os.path.join(download_dir, file_name)
            result = self.download_mp3(path=mp3_path, ref=ref)
            if result == 1:
                return 1
        sys.stdout.write('\n')
        sys.stdout.flush()
        self.session.driver.close()
        return "Finished"

    def search_by_ref(self, ref):
        self.session.driver.get(self.url)
        self.session.driver.ensure_element_by_name('refEr').send_keys(ref)
        self.session.driver.ensure_element_by_id('button-1009').click()

    def change_date_format(self, date):
        try:
            correct_string = date.strptime(str(date.date()), '%Y-%m-%d').strftime('%d-%m-%Y')
            return correct_string
        except Exception as e:
            raise e

    def change_time_format(self, date):
        try:
            correct_string = date.strptime(str(date.hour) + ':' + str(date.minute), "%H:%M").strftime("%I:%M %p")
            if correct_string[0] == "0":
                return correct_string[1::]
            else:
                return correct_string
        except Exception as e:
            raise e

    def ceil_dt(self, dt, delta):
        """Round up to the nearest half hour"""
        return dt + (datetime.datetime.min - dt) % delta

    def set_range(self, now):
        """
        Takes current datetime and finds the nearest, previous half hour.
        Returns the appropriate start and end times and date
        """
        # Format: '10-19-2018'
        # Format: '12:00 AM'
        hour_ago = now - datetime.timedelta(minutes=60)
        rounded = self.ceil_dt(hour_ago, datetime.timedelta(minutes=30))

        start_date = self.change_date_format(rounded)
        start_time = self.change_time_format(rounded)
        thirty_mins = datetime.timedelta(minutes=30)
        end_date = start_date
        end_time = self.change_time_format(rounded + thirty_mins)
        return (start_date, start_time, end_date, end_time)

    def search_by_range(self, start_date, start_time, end_date, end_time):
        """ Doesn't work correctly. Date seems to work but time not so much.

        Search records on www.prosodie.com by date range and return session.
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
        if start_date:
            self.browser.find_element_by_name('dateDebut').send_keys(start_date)
        if start_time:
            self.browser.find_element_by_name('heureDebut').send_keys(start_time)
        if end_date:
            self.browser.find_element_by_name('dateFin').send_keys(end_date)
        if end_time:
            self.browser.find_element_by_name('heureFin').send_keys(end_time)
        self.browser.find_element_by_id('button-1009').click()
        return

    def download_all_half_hour(self):
        self.logger.debug(f"Downloading calls from last half hour")
        self.logger.debug(f"Login check...")
        if not self.validated:
            self.logger.debug(f"Not logged in. Validating")
            self.login_selenium()
        self.logger.debug(f"Logged in.")
        self.logger.debug(f"Getting search range")
        search_range = self.set_range(datetime.datetime.now())
        sleep(2)
        self.logger.debug(f"Applying filters")
        self.browser.find_element_by_id("criteres-inputEl").send_keys('_EN')
        self.search_by_range(*search_range)
        sleep(5)
        self.logger.debug(f"Downloading results to {self.download_path}")
        csvB = self.browser.find_element_by_id("csvButton")
        csvB.click()
        self.browser.find_element_by_id("button-1006").click()
        self.browser.switch_to.window(self.browser.window_handles[1])
        sleep(5)
        self.logger.debug(f"Ending session")


if __name__ == '__main__':
    from dotenv import load_dotenv
    from time import sleep
    from pathlib import Path
    module_logger = logging.getLogger('odigo_downloader')
    module_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    module_logger.addHandler(ch)

    load_dotenv()
    username = os.environ.get('PROSODIE_USERNAME')
    password = os.environ.get('PROSODIE_PASSWORD')
    download_dir = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\csvs')
    module_logger.debug(f"Download dir: {download_dir}")
    csv_file = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\csvs\demo\odigo4isRecorder_20190131-162007.csv')

    dl = Downloader(
        username,
        password,
        driver_path=r'C:\Users\RSTAUNTO\Desktop\Projects\rightcall\chromedriver.exe',
        download_path=str(download_dir),
        headless=True,
        logger=module_logger)
    # dl.download_mp3_by_csv(csv_file, download_dir=download_dir)
    dl.download_all_half_hour()
