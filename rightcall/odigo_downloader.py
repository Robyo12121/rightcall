import logging
from requestium import Session
import time
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os


class Downloader():

    def __init__(
            self,
            username,
            password,
            driver_path,
            download_path=None,
            browser='chrome',
            webdriver_options={'arguments': ['headless']}
    ):

        self._username = username
        self._password = password
        self.driver_path = driver_path
        self.download_path = download_path
        self.logger = logging.getLogger('rightcall.downloader')
        self.url = 'https://enregistreur.prosodie.com/odigo4isRecorder/EntryPoint?serviceName=LoginHandler'
        self.browser = browser
        self.webdriver_options = webdriver_options
        self.session = Session(
            webdriver_path=self.driver_path,
            browser=self.browser,
            default_timeout=15,
            webdriver_options=self.webdriver_options
        )
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('rightcall.odigo_downloader')

    def __str__(self):
        return f"\nDOWNLOAD PATH: {self.download_path}\nOPTIONS: {self.webdriver_options}\n" \
            f"DRIVER PATH: {self.driver_path}\nUSERNAME: {self._username}\nURL: {self.url}"

    def login(self):
        self.session.driver.get(self.url)
        self.session.driver.ensure_element_by_name('mail').send_keys(self._username)
        self.session.driver.ensure_element_by_name('password').send_keys(self._password)
        self.session.driver.ensure_element_by_name('valider').click()

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
        self.login()
        self.search_by_ref(ref)
        result = self.download_mp3(path, ref)
        if result == 1:
            return 1
        self.session.driver.close()

    def download_mp3_by_csv(self, csv_path, download_dir=None):
        if download_dir is None:
            download_dir = self.download_path
        self.login()
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


if __name__ == '__main__':
    from dotenv import load_dotenv
    from time import sleep
    from pathlib import Path
    load_dotenv()
    username = os.environ.get('PROSODIE_USERNAME')
    password = os.environ.get('PROSODIE_PASSWORD')
    download_dir = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\mp3s\demo')
    print(download_dir)
    csv_file = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\csvs\demo\odigo4isRecorder_20190131-162007.csv')
    dl = Downloader(
        username,
        password,
        r'C:\Users\RSTAUNTO\Desktop\Projects\rightcall\chromedriver.exe',
        webdriver_options={})
    dl.download_mp3_by_csv(csv_file, download_dir=download_dir)
