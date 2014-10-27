import requests
import pandas as pd
import os
import csv
import datetime
import numpy as np
from lxml import etree
from collections import deque

# TODO: get pushkey from csv file


# example URL:
# "http://clmain.ekmmetering.com/14703~1000@1407875379561.xml?OTE0ODg1OjgxMDAxNQ"
# see: http://www.ekmmetering.com/developer-portal  

#Output directory
meterdataroot = "./Billing Data"

#URL building variables
baseurl = "http://clmain.ekmmetering.com/"
#meternumber filled in dynamically
numberofreads = 1000;
xmlextension = ".xml?"

# List of fields we care about ie. all of them
tag_list = [ "seq", "errCode", "model", "fwVer", "PT", "T1_PT", "T2_PT",
 "T3_PT", "T4_PT", "L1_V", "L2_V", "L3_V", "L1_I", "L2_I", "L3_I", "P", "L1_P",
 "L2_P", "L3_P", "max_P", "max_P_period", "CT_ratio", "P1_count", "P2_count",
 "P3_count", "P1_ratio", "P2_ratio", "P3_ratio"]

def download_file(url, local_filename):
    #local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return 

def download_meterdata(metermetadata, metersequencedata):
	customerColumn = 'Customer Name'
	meterColumn = 'Meter Name'
	meterNumberColumn = 'Meter Number'
	pushKeyColumn = "Push Key"
	seqNumberColumn = "Last Sequence"
	df = pd.read_csv(metermetadata)
	seqdf = pd.read_csv(metersequencedata)
	# Make sure metermetadata has the needed columns!
	assert customerColumn in df
	assert meterColumn in df
	assert pushKeyColumn in df

	# Make sure metersequencedata has the needed columns!
	assert meterNumberColumn in seqdf
	assert seqNumberColumn in seqdf

	joineddf = pd.concat([df.ix[:,['Meter Name', 'Push Key']],seqdf],join='inner',axis=1)
	newseqarray = np.zeros(0)
	print joineddf

	for index, row in joineddf.iterrows():
		customer = row[customerColumn]
		meter = row[meterColumn]
		meterid = row[meterNumberColumn]
		pushkey = row[pushKeyColumn]
		print 'PushKey is %s' % (pushkey,)
		# 
		lastseq = row[seqNumberColumn]
		meterdir = '%s/%s/%s' % (meterdataroot,customer, meter,)
		if not os.path.exists(meterdir):
			os.makedirs(meterdir)
		print 'lastseq is currently ' + str(lastseq)
		filename = '%s/%s/%s/%s' % (meterdataroot,customer, meter, meterid)
		url = "".join((
			baseurl,
			str(meterid), 
			"~", 
			str(numberofreads), 
			"@", 
			str(lastseq), 
			xmlextension,
			pushkey))
		print 'Downloading data for Customer: %s Meter: %s from %s' % (customer, meter, url,)
		download_file(url, filename + '.xml')
		#Update the sequence number
		newseq = xml2csv(filename)
		newseqarray = np.append(newseqarray,newseq)
		print 'new sequence number to write to file is ' + newseq
		os.remove(filename + '.xml')
	joineddf[seqNumberColumn] = newseqarray
	joineddf.ix[:, [customerColumn,meterNumberColumn, seqNumberColumn]].to_csv(metersequencedata, index=False)
	print joineddf

def xmlrecordtoarray(read):
	info = []
	for tag in tag_list:
		node = read.get(tag)

		if node is not None:
			info.append(node)
		else:
			info.append("")
	return info

def parsemeterxml(xmlfile):
	out_data = []
	tree = etree.parse(xmlfile)
	meter = tree.getroot().getchildren()[0]

	for read in meter.findall("read"):
		read_info = xmlrecordtoarray(read)
		if read_info:
			out_data.append(read_info)
	return out_data

def xml2csv(filename):
	data = parsemeterxml(filename + '.xml')
	#If a file for this month already exists, don't write a header

	# Note that the timestamp in the xml file is a UTC with an additional
	# three numbers which are fractions of seconds
	monthnumber = datetime.datetime.fromtimestamp(int(data[-1][0][:-3])).month
	yearnumber = datetime.datetime.fromtimestamp(int(data[-1][0][:-3])).year
	outfile = '%s_%s_%s.csv' % (filename, monthnumber, yearnumber,)

	lastrecordtime = datetime.datetime.combine(datetime.date.min, datetime.time.min)
	if not os.path.exists(outfile):
		out_file = open(outfile, 'w+')
		csv_writer = csv.writer(out_file)
		csv_writer.writerow(tag_list)
	else:
		df = pd.read_csv(outfile)
		#lastrecordtime = datetime.datetime.fromtimestamp(int(str(df['seq'].min())[:-3]))
		#print lastrecordtime
		out_file = open(outfile, 'a')
		csv_writer = csv.writer(out_file)

	for row in data:
		#if (datetime.datetime.fromtimestamp(int(row[0][:-3])) > lastrecordtime):
		csv_writer.writerow(row)
	out_file.close()

	return data[0][0]


download_meterdata("Meter Names.csv", "Meter Sequencer.csv")





