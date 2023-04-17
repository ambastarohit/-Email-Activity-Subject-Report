INTRODUCTION: 

--------    Email Activity Subject Report    ---------

1. Goal is to have a repeatable analysis of customer emails to account executives or account managers. We want to have an understand of who is reaching out to us (what customer and about what product line) and what is the reason for the email (request, problem, question). 


PRE REQUISITE:

1. Install Python (version 10 preferred) from open-source.

2. Install all the neccessary libraries using command terminal before running the script, or else the script won't run.
Install these by running each at a time.

      pip install json
	pip install psycopg2
	pip install pandas 
	pip install sqlalchemy
	pip install styleframe
	pip install --user --upgrade "sqlalchemy<2.0"


2. Before running the report check the 'config_Customer_Email_Analysis' json file. The report will be generated based on the dates mentioned under "filter_system_modstamp". The report is eventually generated from the date mentioned here to till date.


USES:

1. Run the script on command terminal using the below command:

	python "Email Activity Subject Report" config_Customer_Email_Analysis

This will automatically generate the report in user's local drive space (C: drive)