import json
from bson import json_util
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import hashlib


class Website:
    """ An object which stores websites to check and the measurement results.

    Keyword Arguments:
        url (str): URL of the website to check
        regex (str, optional): Regular expression to search on website
    """
    def __init__(self, url, regex=""):
        self.url = url
        self.status = 0
        self.regex = regex
        self.resp_time = 0
        self.regex_found = False
        self.regex_set = False
        self.date = None
        self.rep_hash = hashlib.sha256(self.url.encode("utf-8")).hexdigest()

    def get_response(self):
        """ Measures and checks the website.
        """
        try:
            self.date = datetime.utcnow()
            response = requests.get(self.url, timeout=10)
            self.status = response.status_code
            self.resp_time = round(response.elapsed.total_seconds(), 3)

            # check for regex only if set
            if self.regex:
                soup = BeautifulSoup(response.content, 'html.parser')
                if soup.find_all(string=re.compile(self.regex), limit=1):
                    self.regex_found = True
                    self.regex_set = True

        except requests.exceptions.Timeout:
            self.status = 408
        except requests.exceptions.RequestException:
            self.status = 503

    def as_json(self):
        """ Print measurment results as json.

        Returns:
            string: Results of the website measurement
        """
        data_set = {"hash": self.rep_hash, "url": self.url,
                    "status": self.status, "regex_set": self.regex_set,
                    "regex_found": self.regex_found,
                    "response_time": self.resp_time, "date": self.date}
        return json.dumps(data_set, default=json_util.default)
