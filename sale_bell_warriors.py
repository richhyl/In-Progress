"""
You'll need to install:
pip install python-geoip  	(http://pythonhosted.org/python-geoip/)
pip install requests		(http://docs.python-requests.org/en/latest/)

Optionally (if you buy Growl for $3.99):
pip install gntp			(https://pypi.python.org/pypi/gntp)
"""
username = ""
password = "" # super-secure

import requests
import json
import subprocess
import time
import datetime
from time import gmtime, strftime
import os
import csv
from geoip import open_database
try:
	import gntp.notifier
	growl_enabled = True
except:
	growl_enabled = False

subdivision_names = {}
transactions = {}
seen_events = set()
announced_transactions = set()
first_run = True # set this to False to show old purchases as soon as the script runs - useful for testing

def main():
	initialize_growl()	
	load_geoip_data()
	print "Waiting for purchases..."

	while True:
		process_latest_events()

def process_latest_events():
	global first_run
	try:
		query_data =  {
			"query": 
			{
				"filtered": 
				{
					"query": 
					{
						"query_string": 
						{
							"query": "@fields.gameName='warriors' AND (@fields.eventName='iap_verified' OR @fields.eventName='iap_completed')"
						}
					}
				}
			},
			"size": 1000,    
			"sort": 
			[     
				{                    
					"@timestamp": 
					{
						"order": "desc"
					}                
				}                
			]                
		}

		r = requests.get("https://logstash.mindcandy.com:443/logstash-*/_search", data=json.dumps(query_data), auth=(username, password), verify=False)
	except Exception, e:
		print e
		time.sleep(5)
		return

	try:
		data = json.loads(r.text)
	except Exception, e:
		print e# + "\n" + r.text
		time.sleep(5)
		return

	hits = data["hits"]["hits"]

	for hit in hits:
		process_event(hit)

	first_run = False

	time.sleep(15)

def process_event(data):
	fields = data["_source"]["@fields"]
	if "event" not in fields:
		return

	event = data["_source"]["@fields"]["event"]

	# Exclude test events
	if event["isDebug"] == True:
		return

	if "eventName" not in fields:
		return

	eventName = fields["eventName"]

	message_uuid = fields["uuid"]
	if message_uuid in seen_events:
		return

	seen_events.add(message_uuid)

	if first_run:
		return

	if eventName == "iap_completed":
		process_iap_completed_event(fields, message_uuid)
	elif eventName == "iap_verified":
		process_iap_verified_event(fields, message_uuid)

def initialize_growl():
	global growl
	if growl_enabled:
		growl = gntp.notifier.GrowlNotifier(
			applicationName = "Sale Bell",
			notifications = ["Sale"],
			defaultNotifications = ["Sale"]
		)
		growl.register()



def load_geoip_data():
	global ip_db
	ip_db = open_database('GeoLite2-City.mmdb')
	with open("GeoLite2-City-Locations.csv", "rb") as csvfile:
		reader = csv.reader(csvfile)
		for row in reader:
			if row[0] == "geoname_id":
				continue

			geoname_id = int(row[0])
			subdivision_name = row[6].strip()
			city_name = row[7].strip()
			if subdivision_name != "" and city_name != "":
				subdivision_names[geoname_id] = "{}, {}".format(city_name, subdivision_name)
			elif subdivision_name != "":
				subdivision_names[geoname_id] = subdivision_name
			elif city_name != "":
				subdivision_names[geoname_id] = city_name



def get_exchange_rate(currency):
	if currency == "USD":
		return 1.0

	r = requests.get("http://download.finance.yahoo.com/d/quotes.csv?s={}USD=X&f=sl1d1t1ba&e=.csv".format(currency))

	data = r.text.split(",")

	exchange_rate = data[1]

	return float(exchange_rate)

def process_iap_completed_event(data, message_uuid):
	event_fields = data["event"]["fields"]

	ip_address = data["ipAddress"]
	timestamp = data["isoTimestamp"]

	transactionIdentifier = event_fields["transactionIdentifier"]
	receipt = event_fields["receipt"]
	price = event_fields["cost"]
	currency = event_fields["localCurrency"]
	deviceId = data["event"]["deviceId"]
	identityId = data["event"]["identityId"]

	purchase_details = {"transaction_identifier": transactionIdentifier, "timestamp": timestamp, "ip_address": ip_address, "price": price, "currency": currency, "device_id": deviceId, "identity_id": identityId}
	print "{}  {}  iap complete: {}".format(datetime.datetime.now(), message_uuid, transactionIdentifier)
	if transactionIdentifier.strip() == "":
		return

	if transactionIdentifier in transactions:
		transactions[transactionIdentifier] = purchase_details # not actually used, but for completeness sake
		announce_purchase(purchase_details)
	else:
		transactions[transactionIdentifier] = purchase_details

def process_iap_verified_event(data, message_uuid):
	event_fields = data["event"]["fields"]

	transactionIdentifier = event_fields["transactionIdentifier"]
	print "{}  {}  iap verified: {}".format(datetime.datetime.now(), message_uuid, transactionIdentifier)
	if transactionIdentifier in transactions and transactions[transactionIdentifier] != True: # this secondary condition protects us from double validations
		announce_purchase(transactions[transactionIdentifier])
	else:
		transactions[transactionIdentifier] = True


def announce_purchase(purchase_details):
	ip_address = purchase_details["ip_address"]
	price = purchase_details["price"]
	currency = purchase_details["currency"]
	transactionIdentifier = purchase_details["transaction_identifier"]
	timestamp = purchase_details["timestamp"]
	device_id = purchase_details["device_id"]
	identity_id = purchase_details["identity_id"]

	if transactionIdentifier in announced_transactions:
		return

	announced_transactions.add(transactionIdentifier)

	while not price[0].isnumeric():
		price = price[1:]

	while not price[-1:].isnumeric():
		price = price[:-1]

	price = price.replace(",", ".")

	exchange_rate = get_exchange_rate(currency)

	value_in_usd = float(price) * exchange_rate

	lat_long = None
	city = ""
	country = ""
	match = None
	location = None

	try:
		match = ip_db.lookup(ip_address)
	except:
		match = None

	if match == None:
		country = None
		city = None
	else:
		lat_long = match.location
		if "city" in match._data and "geoname_id" in match._data["city"]:
			city_id = match._data["city"]["geoname_id"]
			city = subdivision_names[city_id]
		else:
			city = None
		country = match._data["country"]["names"]["en"]

		location = None
		if country != None and country != "":
			if country == "United States" or country == "United Kingdom":
				if city != None and city != "":
					location = city
				else:
					location = country
			elif city != None and city != "":
				location = "{}, {}".format(city, country)
			else:
				location = country


	print "{}\t{}\t{}\t{}: {} {} (${:.2f}) from {}, {}".format(transactionIdentifier, device_id, identity_id, timestamp, price, currency, value_in_usd, city, country) 
	#print "{},{},{},{},{}".format(transactionIdentifier, identity_id, device_id, value_in_usd, timestamp)

	difference_as_string = "{0:.2f}".format(value_in_usd)
	difference_as_string = difference_as_string.split(".")
	
	dollars = difference_as_string[0]
	cents = ""
	if len(difference_as_string) > 1:
		cents = difference_as_string[1]

	if dollars == "0":
		dollars = ""
	elif dollars == "1":
		dollars = "1 dollar"
	else:
		dollars = "{} dollars".format(dollars)

	if cents == "0" or cents == "00":
		cents = ""
	elif cents == "1":
		cents = "1 cent"
	else:
		cents = "{} cents".format(int(cents))

	if lat_long != None:
		sale_data_for_google_earth = {
			"latitude": lat_long[0],
			"longitude": lat_long[1],
			"amount": "{0:.2f} {1}".format(value_in_usd, currency),
			"transaction_id": transactionIdentifier,
			"location": location

		}

		with open("sales.json", "w") as f:
			f.write(json.dumps(sale_data_for_google_earth))

	if growl_enabled:
		try:
			growl.notify(
				noteType = "Sale",
				title = "Sale!",
				description = "{} {} (${:.2f})\nfrom {}, {}".format(price, currency, value_in_usd, city, country),
				icon = "http://www.mxpolice.com/wp-content/themes/05282011/custom/images/money.png",
				sticky = False,
				priority = 1,
			)

		except Exception, e:
			print e
			pass

	time.sleep(0.5)
	if location == None:
		os.system("say {} {}.".format(dollars, cents) )
	else:
		os.system("say {} {} from {}.".format(dollars, cents, location) )

	time.sleep(5)
	

main()
		
	
