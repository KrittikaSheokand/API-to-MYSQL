#Import packages
import requests
from bs4 import BeautifulSoup
import json
import time
import mysql.connector
import warnings
from datetime import datetime

#ignore warnings
warnings.filterwarnings("ignore")
SQL_DB = "ucdavis"

def create_sql_table(SQL_TABLE_URBAN, SQL_TABLE_URBAN_DEF):
    try:
        #connect to server
        conn = mysql.connector.connect(host='localhost',
                                       user='root',
                                       password='***')
        cursor = conn.cursor()
        
        query = "CREATE DATABASE IF NOT EXISTS " + SQL_DB
        print(query)
        cursor.execute(query);
        
        query = "CREATE TABLE IF NOT EXISTS " + SQL_DB + "." + SQL_TABLE_URBAN + " " + SQL_TABLE_URBAN_DEF + ";";
        print(query)
        cursor.execute(query);

        cursor.close()
        conn.close()
        return

    except IOError as e:
        print(e)

SQL_TABLE_URBAN = "urban"
SQL_TABLE_URBAN_DEF = "(" + \
        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY" + \
        ",login VARCHAR(50)" + \
        ",email VARCHAR(1000)" + \
        ",hireable VARCHAR(50)" + \
        ",bio VARCHAR(1000)" + \
        ",twitter_username VARCHAR(50)" + \
        ",public_repos INT" + \
        ",public_gists INT" + \
        ",followers INT" + \
        ",following INT" + \
        ",created_at VARCHAR(50)" + \
        ")"

create_sql_table(SQL_TABLE_URBAN, SQL_TABLE_URBAN_DEF)

try:
    # connect to server
    conn = mysql.connector.connect(host='localhost',
                                   database='ucdavis',
                                   user='root',
                                   password='***')

    cursor = conn.cursor()

    #Follow the same process as earlier to get information of contributors
    url = "https://api.github.com/repos/apache/hadoop/contributors"
    params = {"per_page": 100}
    page = requests.get(url, params=params)
    doc = BeautifulSoup(page.content, 'html.parser')
    json_dict = json.loads(str(doc))

    #Command to input data - data to be inserted later
    parameterized_stmt = "INSERT INTO " + SQL_TABLE_URBAN + " (id, login, email, hireable, bio, twitter_username, public_repos, public_gists, followers, following, created_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    for i in range(0,100):
        #Get ith contributor information
        json_dict_number = json_dict[i]
        
        #Pull url
        contributor_url = json_dict_number['url']
        
        #Go to the url and get the page
        headers = {'User-agent': 'Mozilla/5.0'} 
        page_contributor = requests.get(contributor_url, headers =headers )
        doc_contributor = BeautifulSoup(page_contributor.content, 'html.parser')

        json_dict_contributor = json.loads(str(doc_contributor))

        #Get all the information needed
        login = json_dict_contributor.get("login")
        id = json_dict_contributor.get("id")
        email = json_dict_contributor.get("email")
        hireable = json_dict_contributor.get("hireable")
        bio = json_dict_contributor.get("bio")
        twitter_username = json_dict_contributor.get("twitter_username")
        public_repos = json_dict_contributor.get("public_repos")
        public_gists = json_dict_contributor.get("public_gists")
        followers = json_dict_contributor.get("followers")
        following = json_dict_contributor.get("following")
        date_string = json_dict_contributor.get("created_at")
        formatted_date = datetime.fromisoformat(date_string[:-1]) 

        #Input data using the command defined earlier
        cursor.execute(parameterized_stmt, (id, login, email, hireable, bio, twitter_username, public_repos, public_gists, followers, following, formatted_date))
        print('id: ',id)
        time.sleep(10)

    conn.commit()
    cursor.close()
    conn.close()
    
except IOError as e:
    print(e)