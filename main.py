import logging
import re
import threading
import time
from datetime import datetime
from queue import Queue, Empty as QueueIsEmpty
from typing import List, Optional
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, quote
from urllib.request import Request, urlopen

import robots


class Sitemap:
    _xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
    _xml_sitemap_schema = 'xmlns="https://www.sitemaps.org/schemas/sitemap/0.9"'
    _MAX_URL_PER_FILE = 50000  # https://www.sitemaps.org/protocol.html

    def __init__(self):
        self._urls = []

    def add_url(self, url: str, date: datetime):
        self._urls.append(
            {
                'loc': url,
                'lastmod': date
            }
        )

    def create_sitemap(self):
        multi_file = len(self._urls) > self._MAX_URL_PER_FILE

        if multi_file:
            files_num = len(self._urls) // self._MAX_URL_PER_FILE
            if len(self._urls) % self._MAX_URL_PER_FILE != 0:
                files_num += 1

            sitemap_files = []
            for i in range(files_num):
                filename = f'sitemap{i + 1}.xml'
                sitemap_files.append(filename)
                self._create_sitemap_file(filename,
                                          self._urls[i * self._MAX_URL_PER_FILE: (i + 1) * self._MAX_URL_PER_FILE])

            self._create_index_sitemap_file(sitemap_files)

        else:
            self._create_sitemap_file('sitemap.xml', self._urls)

    def _create_index_sitemap_file(self, sitemap_file_names):
        with open('sitemap_index.xml', 'w') as f:
            f.write(self._xml_header)
            f.write(f"<sitemapindex {self._xml_sitemap_schema}>")

            for sitemap in sitemap_file_names:
                f.write("<sitemap>")
                f.write(f"<loc> /{sitemap} </loc>")
                f.write("</sitemap>")

            f.write("</sitemapindex>")

    def _create_sitemap_file(self, filename: str, urls):
        with open(filename, 'w') as f:
            f.write(self._xml_header)
            f.write(f"<urlset {self._xml_sitemap_schema}>")

            for url in urls:
                f.write("<url>")
                f.write(f"<loc> {url['loc']} </loc>")
                f.write(f"<lastmod> {url['lastmod'].strftime('%Y-%m-%dT%H:%M:%S+00:00')} </lastmod>")
                f.write("</url>")

            f.write("</urlset>")

    @property
    def url(self):
        return self._urls


class Crawler(threading.Thread):
    link_regex = re.compile(b'<a [^>]*href=[\'|"](.*?)[\'"][^>]*?>')
    invalid_formats = (
        ".epub", ".mobi", ".docx", ".doc", ".opf",
        ".7z", ".ibooks", ".cbr", ".avi", ".mkv",
        ".mp4", ".jpg", ".jpeg", ".png", ".gif",
        ".pdf", ".iso", ".rar", ".tar", ".tgz",
        ".zip", ".dmg", ".exe")
    User_Agent = "PythonCrawler"
    _logger = logging.getLogger('CrawlingWorker')

    def __init__(self,
                 base_url: str,
                 crawl_queue: Queue,
                 crawled: set,
                 excluded: set,
                 sitemap: Sitemap,
                 ):

        threading.Thread.__init__(self)

        self.base_url_str = base_url
        self.base_url_obj = urlparse(base_url)
        self.crawl_queue = crawl_queue
        self.crawled = crawled

        self.sitemap = sitemap
        self.excluded = excluded

        self.robots_parser = None

    def _get_url(self):
        """
        Getting url from crawl queue.
        Return none if queue is empty.
        """
        try:
            current_url = self.crawl_queue.get(block=True, timeout=5)
        except QueueIsEmpty:
            current_url = None

        return current_url

    @staticmethod
    def _prepare_url(url_str):
        """
        Quoting url if contains non-ascii symbols
        """
        url_split = list(urlsplit(url_str))
        url_split[2] = quote(url_split[2])
        url = urlunsplit(url_split)
        return url

    @staticmethod
    def _get_date_from_response(response) -> datetime:
        """
        Try to get last-modified info
        Or set current date
        """
        if 'last-modified' in response.headers:
            date = response.headers['Last-Modified']
        else:
            date = response.headers['Date']

        return datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z')

    @staticmethod
    def _resolve_url_path(path):
        # From https://stackoverflow.com/questions/4317242/python-how-to-resolve-urls-containing/40536115#40536115
        segments = path.split('/')
        segments = [segment + '/' for segment in segments[:-1]] + [segments[-1]]
        resolved = []
        for segment in segments:
            if segment in ('../', '..'):
                if resolved[1:]:
                    resolved.pop()
            elif segment not in ('./', '.'):
                resolved.append(segment)
        return ''.join(resolved)

    def _clean_link(self, link):
        parts = list(urlsplit(link))
        parts[2] = self._resolve_url_path(parts[2])
        return urlunsplit(parts)

    @staticmethod
    def _convert_html_special_chars(link):
        return link \
            .replace("&", "&amp;") \
            .replace('"', "&quot;") \
            .replace("<", "&lt;") \
            .replace(">", "&gt;")

    def _check_robots(self):
        robots_url = urljoin(self.base_url_str, 'robots.txt')
        self.robots_parser = robots.RobotsParser.from_uri(robots_url)

    def _can_fetch(self, link):
        return self.robots_parser.can_fetch("*", link)

    def _exclude_url(self, link):
        for ex in self.excluded:
            if ex in link:
                return False
        return True

    def run(self):
        # load and parse robots if exists
        logging.debug("Started work")
        self._check_robots()

        while True:
            # try get from queue
            current_url_str = self._get_url()
            if not current_url_str:
                break

            # add to crawled or crawling
            self.crawled.add(current_url_str)

            # quoting url when have non ascii
            current_url_str = self._prepare_url(current_url_str)
            current_url_obj = urlparse(current_url_str)

            # prepare request
            request = Request(current_url_str, headers={"User-Agent": self.User_Agent})

            # check is invalid format and get resp
            if not current_url_obj.path.endswith(self.invalid_formats):
                try:
                    response = urlopen(request, timeout=15)
                except Exception as e:
                    logging.debug(f"Unable to open {current_url_str}. {e}")
                    continue
            else:
                response = None

            # handling response
            response_data, date = None, None
            if response is not None:
                try:
                    response_data = response.read()
                    response.close()

                    date = self._get_date_from_response(response)

                except Exception as e:
                    logging.debug(f"An error except when handling response: {e}")
                    continue

                response_url_string = response.geturl()

            else:
                response_url_string = current_url_str
                date = datetime.now()

            # Add url to sitemap
            self.sitemap.add_url(self._convert_html_special_chars(response_url_string), date)

            # if page not loading
            if not response_data:
                continue

            # Find links
            response_url_obj = urlparse(response_url_string)
            links: List[bytes, ...] = self.link_regex.findall(response_data)
            for link_str in links:
                link_str = link_str.decode("utf-8", errors="ignore")

                if link_str.startswith('/'):
                    link_str = response_url_obj.scheme + '://' + response_url_obj[1] + link_str
                elif link_str.startswith('#'):
                    link_str = response_url_obj.scheme + '://' + response_url_obj[1] + response_url_obj[2] + link_str
                elif not link_str.startswith(('http', "https")):
                    link_str = self._clean_link(urljoin(response_url_string, link_str))
                elif link_str.startswith(("mailto", "tel")):
                    continue

                # Remove the anchor part if needed
                if "#" in link_str:
                    link_str = link_str[:link_str.index('#')]

                # Parse the url to get domain and file extension
                link_obj = urlparse(link_str)
                domain_link = link_obj.netloc

                if link_str in self.sitemap.url:
                    continue
                if link_str in self.crawl_queue.queue:
                    continue
                if domain_link != self.base_url_obj.netloc:
                    continue
                if link_obj.path in ["", "/"] and link_obj.query == '':
                    continue
                if "javascript" in link_str:
                    continue
                if link_obj.path.startswith("data:"):
                    continue

                if not self._can_fetch(link_str):
                    continue
                if link_str in self.crawled:
                    continue
                if not self._exclude_url(link_str):
                    continue

                self.crawl_queue.put(link_str)

            self.crawl_queue.task_done()

        logging.debug("Stopped working")


class CrawlingManager:
    _base_url: str
    _crawling_queue: Queue
    _excluded_urls: set
    _crawled_urls: set
    _sitemap: Sitemap
    _crawler_workers: List[Crawler]
    _num_workers: int

    def __init__(self, base_url, *,
                 excluded_urls: Optional[set] = None,
                 num_workers: int = 30):
        self._base_url = base_url
        self._crawling_queue = Queue()
        self._sitemap = Sitemap()
        self._crawled_urls = set([])
        self._crawler_workers = []
        self._num_workers = num_workers

        if excluded_urls is None:
            excluded_urls = set([])
        self._excluded_urls = excluded_urls

        self._crawling_queue.put(self._base_url)

    def run(self):
        start_time = datetime.now()

        for i in range(self._num_workers):
            crawler = Crawler(self._base_url, self._crawling_queue,
                              self._crawled_urls, self._excluded_urls,
                              self._sitemap)
            crawler.setName(f"Crawler-{i}")
            crawler.start()
            self._crawler_workers.append(crawler)

        while threading.active_count() != 1:
            logging.info(f"Current urls count: {len(self._sitemap.url)}")
            time.sleep(2)

        stop_time = datetime.now()

        logging.info(f"Crawling time: {stop_time - start_time}")
        logging.info(f"Total crawled urls: {len(self._sitemap.url)}")
        self._sitemap.create_sitemap()
        logging.info("Sitemap files successful created in same directory")


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] [%(threadName)s] %(message)s')
    mng = CrawlingManager('https://crawler-test.com/')
    mng.run()


if __name__ == '__main__':
    main()
