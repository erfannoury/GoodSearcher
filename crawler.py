from Queue import Queue
import threading
import time
from SetQueue import SetQueue
from scraper import Scraper


jsons_dir = 'JSONs' # address to save JSON files
crawl_delay = 20 # seconds
thread_cnt = 2

crawledQ = SetQueue()
frontlineQ = SetQueue()

class crawlerThread(threading.Thread):
    def __init__(self, threadID, timeout):
        """
        Constructor for the crawler thread class
        """
        threading.Thread.__init__(self)
        self.ID = threadID
        self.timeout = timeout
        print 'Thread', threadID, ' created at ', time.ctime(time.time())

    def run(self):
        print 'Thread', self.ID, ' starting at ', time.ctime(time.time())
        crawl(self.timeout, self.ID)
        print 'Thread', self.ID, ' exiting at ', time.ctime(time.time())



def crawl(timeout, threadID):
    """
    This method will do the crawling on a URL. In a (almost-)never-ending loop, it will try to get a URL out of the frontline queue, it will crawl it and add new URLs to the frontline queue.

    Parameters
    ----------
    timeout: time object
             this is the timeout time, past which crawling will be stopped
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

def main():
    """

    """
    timeout = time.time() + 2 * 60 #crawl for 5 minutes
    initializeFrontline()
    threads = []
    print 'creating threads'
    for i in range(thread_cnt):
        t = crawlerThread(i, timeout)
        threads.append(t)

    print 'starting threads'

    for i in range(thread_cnt):
        threads[i].start()

    for i in range(thread_cnt):
        threads[i].join()

    print 'Exiting main thread'

if __name__ == '__main__':
    main()
