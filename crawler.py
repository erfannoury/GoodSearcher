from Queue import Queue
import threading
import time
from SetQueue import SetQueue
from scraper import Scraper
import argparse
import glob
import json
jsons_dir = 'JSONs' # address to save JSON files

crawledQ = SetQueue()
frontlineQ = SetQueue()

class crawlerThread(threading.Thread):
    def __init__(self, threadID, timeout, crawl_delay):
        """
        Constructor for the crawler thread class
        """
        threading.Thread.__init__(self)
        self.ID = threadID
        self.timeout = timeout
        self.delay = crawl_delay
        print 'Thread', threadID, ' created at ', time.ctime(time.time())

    def run(self):
        print 'Thread', self.ID, ' starting at ', time.ctime(time.time())
        crawl(self.timeout, self.delay, self.ID)
        print 'Thread', self.ID, ' exiting at ', time.ctime(time.time())



def crawl(timeout, crawl_delay, threadID):
    """
    This method will do the crawling on a URL. In a (almost-)never-ending loop, it will try to get a URL out of the frontline queue, it will crawl it and add new URLs to the frontline queue.

    Parameters
    ----------
    timeout: time object
             this is the timeout time, past which crawling will be stopped
    crawl_delay: int
                timeout between two crawling attempts
    threadID: int, str
              this is the ID of the thread that is using this method. for logging purposes only.
    """
    while True:
        if time.time() > timeout:
            break
        if frontlineQ.empty():
            time.sleep(crawl_delay)
            continue
        url = frontlineQ.get()
        try:
            print 'Thread', threadID, ' scraping ', url
            sc = Scraper(url)
            sc.writeJSON(jsons_dir)
            outgoings = sc.getBookLinks()
            crawledQ.put(url)
            for u in outgoings:
                if not crawledQ.contains(u):
                    frontlineQ.put(u)
        except:
            crawledQ.put(url)
        frontlineQ.task_done()
        time.sleep(crawl_delay/2)




def initializeFrontline():
    """
    This method will initialize the frontline queue and also frontline list.
    It can be modified to resume crawling after the last crawl operation.
    At the moment, the latter feature is not implemented.
    """
    seed = 'http://www.goodreads.com/book/show/3241368-the-little-prince-letter-to-a-hostage'
    frontlineQ.put(seed)
    all_jsons_dir = glob.glob(jsons_dir + '/*.json')
    if len(all_jsons_dir) > 0:
        for jdir in all_jsons_dir:
            with open(jdir, 'r') as f:
                jsn = json.load(f)
                crawledQ.put(jsn['url'])
                for outl in jsn['outlinks']:
                    if not crawledQ.contains(outl):
                        frontlineQ.put(outl)
    print crawledQ.qsize(), ' urls crawled already.'
    print 'initial frontline queue contains ', frontlineQ.qsize(), ' urls.'

def main(thread_cnt = 5, crawl_delay = 20, timeout=2):
    """
    crawler module.
    Parameters
    ----------
    thread_cnt: int
                number of threads to use for crawling
    crawl_delay: int
                seconds between two crawling attempts, for each thread
    timeout: int
                crawling duration, after which crawling will be halted. In minutes
    """
    timeout = time.time() + timeout * 60
    initializeFrontline()
    threads = []
    print 'creating threads'
    for i in range(thread_cnt):
        t = crawlerThread(i, timeout, crawl_delay)
        threads.append(t)

    print 'starting threads'

    for i in range(thread_cnt):
        threads[i].start()

    for i in range(thread_cnt):
        threads[i].join()

    print 'Exiting main thread'

if __name__ == '__main__':
    """
    Main entry for the crawling module. Arguments for number of threads, delay between two crawl attempts and crawling duration are required.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threads", type=int, help="Number of threads")
    parser.add_argument("-d", "--delay", type=int, help="Delay between every two crawl attempts in seconds")
    parser.add_argument("-e", "--timeout", type=int, help="Crawling duration, after which crawling will be stopped. In minutes")
    args = parser.parse_args()
    if (not args.threads) or (not args.delay) or (not args.timeout):
        parser.print_help()
        exit()
    main(args.threads, args.delay, args.timeout)
