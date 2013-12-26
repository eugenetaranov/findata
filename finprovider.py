#!/usr/bin/env python

import sys
import cymysql
from argparse import ArgumentParser
from time import sleep, strftime
from signal import signal, SIGINT
from urllib2 import Request, urlopen, HTTPError, URLError
from urllib import urlencode
import pdb

###VARIABLES
DBUSER = 'forex'
DBPASSWORD = 'finance'
DBHOST = '127.0.0.1'
DBPORT = 3306
DB = 'forex'
QUOTESTABLE = 'Quotes'
VERBOSE = False
## set flag to Up or Down if AVG(CHANGE) for LASTTICKS is less than (currPrice - prevPrice) * FACTOR
FACTOR = 2
LASTTICKS = 1000
###
SYMBOL_MAP = {'yahoo': {'stocks': ['AAPL', 'GOOG', 'YHOO'], 'currency': ['EURUSD', 'GBPUSD'] }}
###


def signal_handler(signal, frame):
    print '\nExiting...'
    exit(1)

def parseargs():
    parser = ArgumentParser()
    parser.add_argument('--history', action = 'store_true', help = 'Get historical data' )
    parser.add_argument('--startdate', help = 'Start date, YYYY-MM-DD')
    parser.add_argument('--enddate', help = 'End date, YYYY-MM-DD')
    parser.add_argument('--realtime', action = 'store_true', help = 'Get realtime data')
    parser.add_argument('--dbinit', action = 'store_true', help = 'Reset realtime table')
    parser.add_argument('--symbol', help = 'Symbol')
    parser.add_argument('--sleep', type = int, help = 'Sleep for N seconds')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'Verbose mode')
    parser.add_argument('-t', '--trace', action = 'store_true', help = 'Trace mode')
    return vars(parser.parse_args())

class dbConnect():
    def __init__(self, host, user, passwd, db, port):
        try:
            conn = cymysql.connect(host, user, passwd, db, port, charset = 'utf8')
            self.cursor = conn.cursor()
        except cymysql.err.OperationalError:
            print "Error connecting to database"
            sys.exit(1)

    def __del__(self):
    	try:
        	self.cursor.close()
        except AttributeError:
        	pass

    def runsql(self, sql):
    	# if VERBOSE: print "Executing SQL: %s" % sql
    	self.cursor.execute(sql)
        return self.cursor.fetchall()

class dbManager(dbConnect):
	def createRTable(self, tablename):
		"""Create real time table for quotes"""
		self.runsql("CREATE TABLE %s (Rec INT NOT NULL AUTO_INCREMENT PRIMARY KEY, Date DATE, Time TIME, Symbol VARCHAR(10), Bid FLOAT, \
			Ask FLOAT, Shift FLOAT, Flag CHAR(1)); COMMIT;" % tablename)

	def dropRTable(self, tablename):
		try:
			self.runsql("DROP TABLE %s; COMMIT;" % tablename)
		except cymysql.err.InternalError:
			pass

	def createTable(self, tablename):
		"""Create table for historical prices"""
		self.runsql("CREATE TABLE %s (Rec INT NOT NULL AUTO_INCREMENT PRIMARY KEY, Date DATE, Time TIME, Open FLOAT, Close FLOAT, High FLOAT, \
			Low FLOAT, Adj_close FLOAT, Volume INT); COMMIT;" % tablename)

	def dropTable(self, tablename):
		try:
			self.runsql("DROP TABLE %s;" % tablename)
		except cymysql.err.InternalError:
			pass

	def addQuotes(self, values):
		self.runsql("INSERT INTO %(table)s (Date, Time, Symbol, Bid, Ask, Shift, Flag) VALUES \
			('%(date)s', '%(time)s', '%(symbol)s', %(bid)s, %(ask)s, %(shift)s, '%(flag)s'); COMMIT;" % values)

	def addHistoricalPrice(self, values):
		self.runsql("INSERT INTO %(table)s (Date, Open, Close, High, Low, Adj_close, Volume) VALUES \
			('%(Date)s', %(Open)s, %(Close)s, %(High)s, %(Low)s, %(Adj Close)s, %(Volume)s); COMMIT;" % values)

	def getPreviousPrice(self, symbol):
		res = self.runsql("SELECT Bid FROM %(tablename)s WHERE symbol LIKE '%(symbol)s' ORDER BY \
			Rec DESC LIMIT 1;" % {'tablename': QUOTESTABLE, 'symbol': symbol})
		try:
			prevPrice = float(res[0][0])
		except:
			prevPrice = None
		return prevPrice

	def getAvgShift(self, symbol, ticks):
		res = self.runsql("SELECT AVG(Shift) FROM %(tablename)s WHERE Symbol LIKE '%(symbol)s' ORDER BY \
			Rec LIMIT %(ticks)s;" % {'tablename': QUOTESTABLE, 'symbol': symbol, 'ticks': ticks})
		try:
		 	avg = float(res[0][0])
		except:
			avg = 0
		return avg

class Provider(object):
	"""Generic quotes provider"""
	def __init__(self, symbol, kind = None, sdate = None, edate = None, period = None):
		""" symbol: any commodity like gold or google stocks """
		#maps symbols to providers
		for provider, data in SYMBOL_MAP.iteritems():
			for commodityType, symbols in data.iteritems():
				if symbol in symbols:
					self.symbol = symbol
					self.provider = provider
					self.commodityType = commodityType
					return
		raise Exception, "Error: provided symbol '%s' is not included in the SYMBOL_MAP" % symbol


class Provider_YAHOO(Provider):
	"""YAHOO quotes provider"""
	def __init__(self, symbol, kind = None, sdate = None, edate = None, period = None):
		super(Provider_YAHOO, self).__init__(symbol, kind, sdate, edate, period)
		self.url_currency = 'http://download.finance.yahoo.com/d/quotes.csv?s=%s=X&f=sl1d1t1c1&e=.csv'
		self.url_stocks = 'http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=b2,b3,b4'

	def getRTPriceCurrency(self):
		""" Provides latest price for currency, sets date and time """
		res = urlopen(self.url_currency % self.symbol)
		price, date, time = res.read().split(',')[1:4]
		price = float(price)
		date, time = strftime("%Y-%m-%d"), strftime("%H:%M:%S")
		return {'date': date, 'time': time, 'bid': price, 'ask': price, 'table': QUOTESTABLE, \
			'symbol': self.symbol}

	def getRTPriceStocks(self):
		""" Provides latest prices (Bid/Ask) for stock, sets date and time """
		res = urlopen(self.url_stocks % self.symbol)
		ask, symbol, bid, symbol, vol = res.read().split(',')
		vol = float(vol) ##Not used
		date, time = strftime("%Y-%m-%d"), strftime("%H:%M:%S")
		return {'date': date, 'time': time, 'bid': float(bid), 'ask': float(ask), \
			'table': QUOTESTABLE, 'symbol': self.symbol}

	def _requestHPricesStocks(self, symbol, start_date, end_date):
		"""
		Get historical prices for stocks.
		Date format is 'YYYY-MM-DD'
		Returns a nested dictionary (dict of dicts).
		outer dict keys are dates ('YYYY-MM-DD')
		"""
		params = urlencode({
		    's': symbol,
		    'a': int(start_date[5:7]) - 1,
		    'b': int(start_date[8:10]),
		    'c': int(start_date[0:4]),
		    'd': int(end_date[5:7]) - 1,
		    'e': int(end_date[8:10]),
		    'f': int(end_date[0:4]),
		    'g': 'd',
		    'ignore': '.csv',
		})
		url = 'http://ichart.yahoo.com/table.csv?%s' % params
		req = Request(url)
		resp = urlopen(req)
		content = str(resp.read().decode('utf-8').strip())
		daily_data = content.splitlines()
		hist_dict = dict()
		keys = daily_data[0].split(',')
		for day in daily_data[1:]:
		    day_data = day.split(',')
		    date = day_data[0]
		    hist_dict[date] = \
		        {keys[1]: day_data[1],
		         keys[2]: day_data[2],
		         keys[3]: day_data[3],
		         keys[4]: day_data[4],
		         keys[5]: day_data[5],
		         keys[6]: day_data[6]}
		return hist_dict

	def printDetails(self):
		print "Symbol: %s, type: %s, API URL: %s" % (self.symbol, self.commodityType, \
				(self.url_currency % self.symbol))

	def getHistoricalPrices(self, dbm, sdate = '1971-01-01', edate = '2013-12-21', period = 'd'):
		"""
		provides historical prices for period specified
		type: 'rt' - realtime quotes, 'h' - historical data
		sdate: applies to type = 'h'; start date formatted YYYY-MM-DD
		edate: applies to type = 'h'; end date formatted YYYY-MM-DD
		period: applies to type = 'h'; data resolution - 'm' for minute, 'h' for hour, 'd' for day,
		'w' for week, 'M' for month
		"""
		if self.commodityType == 'currency':
			print 'Not implemented yet'
			sys.exit(1)
		elif self.commodityType == 'stocks':
			try:
				data = self._requestHPricesStocks(self.symbol, sdate, edate)
			except HTTPError, e:
				if e.code == 404:
					print 'Incorrect symbol'
					sys.exit(1)
			for rec in data.iteritems():
				values = rec[1]
				values['table'] = params['symbol']
				values['Date'] = rec[0]
				dbm.addHistoricalPrice(values)
		return values


def getFlag(dbm, symbol, currPrice):
	flag = 'N'
	prevPrice = dbm.getPreviousPrice(symbol)
	avgShift = dbm.getAvgShift(symbol, LASTTICKS)
	if prevPrice:
		shift = currPrice - prevPrice
		if (shift > 0) and (abs(shift) > avgShift * FACTOR): flag = 'U'
		if (shift < 0) and (abs(shift) > avgShift * FACTOR): flag = 'D'
		shift = round(abs(shift), 4)
	else:
		shift = 0
	return {'shift': shift, 'flag': flag}

def main():
	if params['history']:
		symbol = params['symbol']
		dbm = dbManager(DBHOST, DBUSER, DBPASSWORD, DB, DBPORT)
		dbm.dropRTable(symbol)
		dbm.createTable(symbol)
		commodity = Provider_YAHOO(symbol)
		commodity.getHistoricalPrices(dbm, params['startdate'], params['enddate'])
	elif params['realtime']:
		if params['dbinit']:
			dbm = dbManager(DBHOST, DBUSER, DBPASSWORD, DB, DBPORT)
			dbm.dropRTable(QUOTESTABLE)
			dbm.createRTable(QUOTESTABLE)
			sys.exit(0)
		else:
			while True:
				for provider in SYMBOL_MAP.iterkeys():
					for commodity in SYMBOL_MAP[provider]:
						for symbol in SYMBOL_MAP[provider][commodity]:
							if provider == 'yahoo':
								try:
									commodity = Provider_YAHOO(symbol)
								except Exception, e:
									print e, sys.exit(1)
								if params['sleep'] < 1:
									sleepTime = 1
								else:
									sleepTime = params['sleep']
								if VERBOSE: commodity.printDetails()
								dbm = dbManager(DBHOST, DBUSER, DBPASSWORD, DB, DBPORT)
								try:
									if commodity.commodityType == 'currency':
										values = commodity.getRTPriceCurrency()
										values.update(getFlag(dbm, commodity.symbol, values['bid']))
									elif commodity.commodityType == 'stocks':
										values = commodity.getRTPriceStocks()
										values.update(getFlag(dbm, commodity.symbol, values['bid']))
								except URLError, e:
									print 'Could not obtain data for %s' % commodity.symbol
								dbm.addQuotes(values)
								if VERBOSE: print values
								del dbm
				sleep(sleepTime)


if __name__ == '__main__':
	params = parseargs()
	if params['verbose']: VERBOSE = True
	signal(SIGINT, signal_handler)
	if params['trace']: pdb.set_trace()
	main()