#!/usr/bin/env python

from lxml import html
from lxml import etree
import requests
import base64
import threading
import logging

log = logging.getLogger("webscraper")
logging.basicConfig(filename='webscraper.log',level=logging.INFO)
rooturl = "http://buyingguide.winemag.com"
max_count = 1000
thread_count = 8
#globally modified by threads
wine_set = set()
csv_header = "wine_name,rating,wine_price,varietal,appellation,winery,alcohol,production,bottle_size,category,importer,review)\n"
mutex = threading.Lock()
csv_file_name = 'wineinfo.csv'

def get_item(item_list):
	if len(item_list) > 0:
		return item_list[0].encode("utf-8").strip().replace('\r','').replace('\n', '')
	else:
		return ""

def get_all_items(item_list):
	return ";".join(item_list).encode("utf-8").strip().replace('\r','').replace('\n', '')

def processWineThread ( offset,  total, winelist, csv_file):
	thread_name = "Thread-"+str(offset)
	log.info("Starting process wine Thread #" + str(offset) )
	log.info("Size of wine list is "+ str(len(winelist)))
	count = offset 
	while count < len(winelist):
		wine_link = winelist[count]
		wine_url = rooturl + wine_link
		page = requests.get(wine_url)
		tree = html.fromstring(page.text)

		wine_name = get_all_items( tree.xpath('//div[@class="catalog-header"]//h1/text()')) 

		rating = get_item(tree.xpath('//span[@class="rating"]/text()'))

		wine_price = get_item(tree.xpath('//span[@id="price"]/text()') )

		varietal = get_all_items( tree.xpath('//span[@id="varietals"]/a/text()') )

		appellation  = get_all_items( tree.xpath('//span[@id="appellation"]/a/a/text()') )

		winery = get_item( tree.xpath('//span[@id="brand"]/a/text()') )

		alcohol = get_item( tree.xpath('//span[@id="alcohol"]/text()') )

		production = get_item( tree.xpath('//span[@id="caseProduction"]/text()') )

		bottle_size = get_item( tree.xpath('//span[@id="bottleSize"]/text()') )


		category = get_all_items(tree.xpath('//span[@id="category"]/a/text()'))

		importer = get_item( tree.xpath('//span[@id="importer"]/text()') )

		reviewList = tree.xpath('//div[@itemprop="reviewBody"]/p/text()')
		review = ""
		if len(reviewList) > 0:
			reviewText = reviewList[0].encode("utf-8")
			review = base64.b64encode(reviewText)

		line = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (wine_name , rating, wine_price, varietal, appellation, winery, alcohol,production,bottle_size,category,importer,review)
		log.info("(" + thread_name + ") line is=" +line)

		csv_file.write(line)
		count += total
		log.info("new count is "+ str(count))



def processSearchPage(varietal, search_url):
	global wine_set
	log.debug( "Process Search Page" + varietal) 
	count= 0
	while search_url != None and count < max_count:
		log.debug( search_url )
		page = requests.get(search_url)
		#print page.text
		tree = html.fromstring(page.text)
		for element in tree.xpath('//div[@class="card wines"]/a[not(@class)]'):
			count+=1
			winepath =  element.get("href")
			log.info("adding "+ winepath + "to wineset")
			wine_set.add(winepath)  

		next_page = tree.xpath('//div[@class="next-page-set"]')
		if len(next_page) > 0:
			search_url = rooturl + next_page[0][0].get("href")
		else:
			search_url = None
	log.info("Size of thread wine_set "+ str(len(wine_set) ) )



def main():
	log.info("Starting webscraper")
	log.info("Max count for each varietal is " + str(max_count) )

	varietal_link = rooturl + "/varietals"
	page = requests.get(varietal_link)
	tree = html.fromstring(page.text)
	varietal_map = dict()
	for element in tree.xpath('//div[@class="link-set columnize"]/label'):
		varietal_name = element[0].get("title")
		varietal_link = element[0].get("href")
		varietal_map[varietal_name] = varietal_link

	log.info( "created varietal map")
	log.info("starting to get list of wines, creating new thread per varietal")

	varietalThreadList = []
	for key, val in varietal_map.iteritems():
		log.info(" starting thread for varietal=" + key)
		search_url = rooturl + val
		t = threading.Thread(target= processSearchPage, args = (key, search_url))
		varietalThreadList.append(t)

	[ t.start() for t in varietalThreadList ]
	[ t.join() for t in varietalThreadList ]

	log.info("finished getting list of wines")
	log.info("Size of wineset is "+ str(len(wine_set)))
	winelist = list(wine_set)
	log.debug("writing header to csv file, header=" + csv_header)
	csv_file = open(csv_file_name, "w+")
	csv_file.write(csv_header)

	wineThreadList = []
	for x in xrange(thread_count):
		t = threading.Thread(target = processWineThread, args =(x, thread_count, winelist, csv_file))
		wineThreadList.append(t)


	[ t.start() for t in wineThreadList ]
	[ t.join() for t in wineThreadList ]

	csv_file.flush()
	csv_file.close()
	log.debug("Closing csv connection")
	log.info("Created file as name=" + csv_file_name)
	log.info("Finished webscraper")

#
#
	#varietallist = [ '/varietals/albarino']
	#processVarietalList(varietallist)

if __name__ == '__main__':
	main()
