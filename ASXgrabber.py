#some helpful links

#http://www.marketindex.com.au/yahoo-finance-api
#http://www.asx.com.au/asx/research/ASXListedCompanies.csv

##########################################################################################
# Description: Pulls down ASX stock market data and loads into a Postgre DB.
# Author: Leigh Sullivan
# Version: 0.1 2016-11-16
# *** TO DO: implement roll back failed transaction functionality
# *** TO DO: implement handling of duplicate imports
# *** TO DO: implement execution automation
##########################################################################################

# Postgre table defintions:
#
#"table_schema","table_name","column_name","data_type","character_maximum_length"
#
#"asx","company","code","character",3
#"asx","company","name","character varying",
#"asx","company","category","character varying",
#"asx","company","download_date","date",
#"asx","data","category","character varying",
#"asx","data","code","character",3
#"asx","data","date","character",10
#"asx","data","name","character varying",
#"asx","data","price","numeric",
#"asx","data","volume","numeric",

import urllib2
import psycopg2
import sys
import datetime

#Function to print text bar, just to avoid ugly print calls.
def fnc_print_text_bar():
	print("==================================================================================")

#Function to find the 'nth' character in a string
def fnc_find_nth(haystack, needle, n):
	found_n_amount = 0
	for i in range(len(haystack)):
		a_char = haystack[i]
		if a_char == needle:
			found_n_amount = found_n_amount + 1
		if found_n_amount == n:
			return i

#Function to test if variable is a number 
def is_number(s):
    try:
        complex(s) # for int, long, float and complex
    except ValueError:
        return False
    return True
			
#Start of main program		
fnc_print_text_bar()
print("")
print("Program started...")
print("")
print("Downloading 'ASXListedCompanies.csv'...")
print("")

#Download list of ASX codes from www.asx.com.au
try:
	str_ASX_list_from_web = urllib2.urlopen("http://www.asx.com.au/asx/research/ASXListedCompanies.csv").read()
except Exception as e:
	print (e)
	print("")
	fnc_print_text_bar()
	sys.exit()

print("Downloaded.")
print("Processing 'ASXListedCompanies.csv...'")

#Create array to store downloaded ASX code data
arr_ASX_list_from_web = []

#Parse ASX code list downloaded from web
for csv_single_row_from_ASX_list in str_ASX_list_from_web.splitlines():

	#Remove commas from company description field
	
	#Get position of where the second double quote ["] character is
	int_pos_of_sec_dbl_quote = fnc_find_nth(csv_single_row_from_ASX_list,'"',2)
	
	#Get position of where the first comma [,] character is
	int_pos_of_first_comma = fnc_find_nth(csv_single_row_from_ASX_list,',',1)
	
	#If the position of the first comma character is before the second double quote then remove it
	if (int_pos_of_first_comma < int_pos_of_sec_dbl_quote):
		csv_single_row_from_ASX_list = csv_single_row_from_ASX_list[:int_pos_of_first_comma -1] + csv_single_row_from_ASX_list[int_pos_of_first_comma +1:]
	
	#Now remove all the double quotes
	csv_single_row_from_ASX_list = csv_single_row_from_ASX_list.replace('"','')
	
	#Append the row to the array
	arr_ASX_list_from_web.append(csv_single_row_from_ASX_list.rstrip('\n').rstrip('\r'))
	
print("Trying to connect to the Postgre database...")

#Define our connection string
str_db_conn_string = "host='localhost' dbname='postgres' user='postgres' password=''"
 
print "Connecting to database\n	->%s:" % (str_db_conn_string)
 
#Connect to database
try:
	conn = psycopg2.connect(str_db_conn_string)
	cursor = conn.cursor()
	print "Connected!"
except Exception as e:
	print (e)
	fnc_print_text_bar()
	sys.exit()

#Insert ASX code list into database

#Variable to for ASX code array iteration
int_ASX_code_array_pos = 0

#Variable to store number of new ASX codes
int_num_of_new_ASX_codes = 0

#Iterate through the downloaded ASX list
for csv_single_comp_frm_ASX_list in arr_ASX_list_from_web:
	
	#Increment array postion
	int_ASX_code_array_pos = int_ASX_code_array_pos + 1
	
	#Data starts at line 4
	if (int_ASX_code_array_pos > 3):
		
		#Get the company name
		str_name_frm_ASX_list = csv_single_comp_frm_ASX_list.split(",")[0]
		
		#Get the ASX code
		str_code_frm_ASX_list = csv_single_comp_frm_ASX_list.split(",")[1]
		
		#Get the category
		str_category_frm_ASX_list = csv_single_comp_frm_ASX_list.split(",")[2]
		
		#Get today's date
		dtm_todays_date = datetime.datetime.now()
		
		#Create query to check if ASX code already exists in the database
		str_SQL_query = "SELECT code FROM asx.company WHERE code = '" + str_code_frm_ASX_list + "' GROUP BY code"
		
		#Execute query
		cursor.execute(str_SQL_query)
		
		#Get results from the query
		str_SQL_query_results = cursor.fetchall()
		
		if not str_SQL_query_results:
			#If no code was returned set the results string to "None" (can this be removed?)
			str_SQL_query_results = "None"
		
		#Test to see if ASX code already exists in database
		if str_SQL_query_results[0][0] == str_code_frm_ASX_list:
			print("ASX code: " + str_code_frm_ASX_list + " already exists in database.")
		#If the code doesn't exist then insert the new ASX code into the database with today's date
		else:
			print("Inserting ASX code: " + str_code_frm_ASX_list + "...")
			str_SQL_query =  "INSERT INTO asx.company (name, code, category, download_date) VALUES (%s, %s, %s, %s);"
			data = (str_name_frm_ASX_list, str_code_frm_ASX_list, str_category_frm_ASX_list, dtm_todays_date)
			cursor.execute(str_SQL_query, data)
			conn.commit()
			#Increase the new ASX code count by one
			int_num_of_new_ASX_codes = int_num_of_new_ASX_codes + 1

print("")
print(str(int_num_of_new_ASX_codes) + " new ASX codes inserted into database.")
print("")
fnc_print_text_bar()
print("")
		 
print("Now let's get a some results from Yahoo Finance and insert them into the database...")
print("")

#Get the number ASX codes from the company table in the database
str_SQL_query = "SELECT COUNT(*) FROM asx.company"
cursor.execute(str_SQL_query)
int_total_num_of_ASX_codes = cursor.fetchall()
#Get the complete list of ASX codes and their category to use for Yahoo web requests
str_SQL_query = "SELECT code, category FROM asx.company GROUP BY code, category"
cursor.execute(str_SQL_query)
str_SQL_query_results = cursor.fetchall()

#Create variables need for Yahoo web request loop

#Used to keep track where in the code list the loop is at
int_num_of_rows_processed = 0
#Each Yahoo web request is asks for data for 50 ASX codes, each of these is a transaction
int_transaction_id = 0
#Used to keep track of how many ASX codes have been appended to Yahoo web request
int_num_of_ASX_codes_for_web_request = 0
#Used to build the Yahoo 
str_yahoo_web_request_query = ""
#This is used to keep track of how many ASX codes from the database don't return any data from Yahoo
intCodeNoDataAvailable = 0

#Database INSERT variables
db_insert_category = ""
db_insert_ASX_code = ""
db_insert_name = ""
db_insert_date = ""
db_insert_price = ""
db_insert_volume = ""

#Iterate through list of ASX codes, after 50 codes are gathered then send a web request to Yahoo
for row in str_SQL_query_results:
	
	#Concatenate list of ASX codes to for web request
	str_yahoo_web_request_query = str_yahoo_web_request_query + row[0] + ".AX+"

	#Increment counter of ASX codes for next web request
	int_num_of_ASX_codes_for_web_request = int_num_of_ASX_codes_for_web_request + 1
	
	#Increment counter of total ASX codes processed
	int_num_of_rows_processed = int_num_of_rows_processed +1
	
	#If the number of ASX codes equals 50 (this could be replaced with a variable) OR
	#the end of the results are reached, then send web request to Yahoo	
	if int_num_of_ASX_codes_for_web_request == 50 or int_num_of_rows_processed == int_total_num_of_ASX_codes[0][0]:
		
		#Increment the transaction ID
		int_transaction_id = int_transaction_id + 1
		
		#Remove the last '+' character from the end of the ASX code string
		str_yahoo_web_request_query = str_yahoo_web_request_query[:-1]
		
		#Update display
		print("Requesting ASX codes: <" + str_yahoo_web_request_query + ">")
		print("")
		
		#Try sending the Yahoo web request
		try:
			yahoo_request_results = urllib2.urlopen("http://download.finance.yahoo.com/d/quotes.csv?s=" + str_yahoo_web_request_query + "&f=snd1l1v").read()
			
			#Update display
			print(yahoo_request_results)
			
			#Iterate through Yahoo web request results line by line
			for str_row_from_yahoo_request_results in yahoo_request_results.splitlines():
			
				#Remove double quotes
				str_row_from_yahoo_request_results = str_row_from_yahoo_request_results.replace('\"','')
				
				#Split the row by comma at the 0 (first field) postion to get the ASX code
				db_insert_ASX_code = str_row_from_yahoo_request_results.split(",")[0][0:3]
			
				#Get the ASX code category from the database
				str_SQL_query = "SELECT category FROM asx.company WHERE code ='" + db_insert_ASX_code +"'"
				cursor.execute(str_SQL_query)
				db_insert_category = cursor.fetchall()
				
				#Split the row by comma at the 1 (second field) postion to get the company name
				db_insert_name = str_row_from_yahoo_request_results.split(",")[1]
				
				#If the company name does not equal "N/A" then proceed by...
				if db_insert_name != "N/A":
					try:
						
						#Converting the date to YYYY-MM-DD format
						db_insert_date = datetime.datetime.strptime(str_row_from_yahoo_request_results.split(",")[2], '%m/%d/%Y').strftime('%Y-%m-%d')
						
						#If the company price is not a number then assign a zero price
						if is_number(str_row_from_yahoo_request_results.split(",")[3]) == False:
							db_insert_price = "0"
						else:
							db_insert_price = str_row_from_yahoo_request_results.split(",")[3]
						
						#If the company volume is not a number then assign a zero volume
						if is_number(str_row_from_yahoo_request_results.split(",")[4]) == False:
							db_insert_volume = "0"
						else:
							db_insert_volume = str_row_from_yahoo_request_results.split(",")[4]
					
					#If the date can not be converted then raise exception and give date 'N/A' value
					except Exception as e:
						print (e)
						db_insert_date = "N/A"
						if is_number(str_row_from_yahoo_request_results.split(",")[3]) == False:
							db_insert_price = "0"
						else:
							db_insert_price = str_row_from_yahoo_request_results.split(",")[3]
						if is_number(str_row_from_yahoo_request_results.split(",")[4]) == False:
							db_insert_volume = "0"
						else:
							db_insert_volume = str_row_from_yahoo_request_results.split(",")[4]
				
				#If company exists insert records into the database
				if db_insert_name != "N/A":
					str_SQL_query =  "INSERT INTO asx.data (category, code, name, date, price, volume) VALUES (%s, %s, %s, %s, %s, %s);"
					data = (db_insert_category[0], db_insert_ASX_code, db_insert_name, db_insert_date, db_insert_price, db_insert_volume)
					cursor.execute(str_SQL_query, data)
					conn.commit()
				else:
					#Else increment counter of ASX codes with no data available
					intCodeNoDataAvailable = intCodeNoDataAvailable + 1
			
			#Reset Yahoo web request variables for new transaction	
			str_yahoo_web_request_query = ""
			int_num_of_ASX_codes_for_web_request = 0
			
			#Update display with progress
			dblPercentComplete = (float(int_num_of_rows_processed) / float(int_total_num_of_ASX_codes[0][0])) *100	
			print("Transactions ID: " + str(int_transaction_id) + ", " + str(int_num_of_rows_processed) + " codes of " + str(int_total_num_of_ASX_codes[0][0]) + " codes processed. %" + str(round(dblPercentComplete,1)) + " Complete..")
			print("")
			fnc_print_text_bar()
			print("")
		
		#Catch any errors when try to receive data from Yahoo web request	
		except Exception as e:
			print (e)

#Update display that the program has finished	
print("All done!")
print("There were " + str(intCodeNoDataAvailable) + " codes with no data available!")
print(str(int_num_of_new_ASX_codes) + " new ASX codes inserted into database.")
print("")