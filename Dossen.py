#Imports
import requests
import re
import time
import json
import os
import pandas as pd

from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor, as_completed

#Cities to crawl
with open("Dossen_villes.txt","r",encoding = "utf-8") as file :
    Villes = file.readlines()
Villes = ["https://en.dossen.com/hotel/list?city="+ville.replace("\n","") for ville in Villes]

#File to stock hotels data
with open('Dossen.csv','w',encoding="utf-8") as fhandle:
        print('url\tnom\tadress\tetoiles\tcapacité',file=fhandle)
        
#Main crawl fonction
def crawl(ville) : 
    chrome_options = Options()
    path = r".\chromedriver_win32\chromedriver.exe"
    chrome_options.add_argument("--headless")
    chrome = webdriver.Chrome(executable_path= path ,options=chrome_options)
    chrome.maximize_window()
    try :
        #Wait for hotels to load
        check_ville = 0
        chrome.get(ville)
        wait = WebDriverWait(chrome, 10)
        loading = wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR,"div.spinner")))
        nb_hotels = chrome.find_element(By.XPATH,'//*[@id="listContainer"]/div/div[3]/div[2]/h3').text
        with open("Check_villes_before_dossen.txt","a",encoding ="utf-8") as file :
            print(nb_hotels,file = file)

        #CRAWL
        #Crawl simple
        lecture_hotels = chrome.find_elements(By.CSS_SELECTOR,"div.hotel-info")
        lecture_noms = [url.find_element(By.TAG_NAME,"h2").text
                           for url in lecture_hotels]
        lecture_adrs = [url.find_element(By.TAG_NAME,"h6").text.split("(")[0]
                           for url in lecture_hotels]

        check_ville += len(lecture_noms)
        for i,nom in enumerate(lecture_noms) : 
            url =""
            adrs = lecture_adrs[i]
            etoiles =""
            rooms = ""
            try:
                result=url+'\t'+ str(nom)+'\t'+ str(adrs)+ '\t'+ str(etoiles)+ '\t' +str(rooms)
                with open('Dossen.csv','a',encoding="utf-8") as fhandle:
                    print(result,file=fhandle)
            except :
                exception=""+'\t'+str(nom)+'\t'+""+'\t'+""+'\t'+""
                with open('Dossen.csv','a',encoding="utf-8") as fhandle:
                    print(exception,file=fhandle)

        #NEXT PAGES
        last_page_not_reached = True
        while last_page_not_reached :
            i = 0
            btn = False
            while True :
                if btn == False and i <3:
                    i += 1
                    try :
                        new_page = chrome.find_element(By.CSS_SELECTOR,"a.next")
                        chrome.execute_script("arguments[0].scrollIntoView();", new_page)
                        chrome.execute_script("arguments[0].click();",new_page)
                        wait = WebDriverWait(chrome, 10)
                        loading = wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR,"div.spinner")))

                        btn = True

                        lecture_hotels = chrome.find_elements(By.CSS_SELECTOR,"div.hotel-info")
                        lecture_noms = [url.find_element(By.TAG_NAME,"h2").text
                                           for url in lecture_hotels]
                        lecture_adrs = [url.find_element(By.TAG_NAME,"h6").text.split("(")[0]
                                           for url in lecture_hotels]

                        check_ville += len(lecture_noms)
                        for i,nom in enumerate(lecture_noms) : 
                            url =""
                            adrs = lecture_adrs[i]
                            etoiles =""
                            rooms = ""
                            try:
                                result=url+'\t'+ str(nom)+'\t'+ str(adrs)+ '\t'+ str(etoiles)+ '\t' +str(rooms)
                                with open('Dossen.csv','a',encoding="utf-8") as fhandle:
                                    print(result,file=fhandle)
                            except :
                                exception=""+'\t'+str(nom)+'\t'+""+'\t'+""+'\t'+""
                                with open('Dossen.csv','a',encoding="utf-8") as fhandle:
                                    print(exception,file=fhandle)

                    except Exception as e:
                        with open('exception.txt',"a") as flog:
                            print('%r could not click next page btn: %s' % (ville, e),file=flog)
                elif btn == True :
                    break
                elif i>=3:
                    with open('exception.txt',"a") as flog:
                        print('%r reached last page: ' % (ville),file=flog)
                    last_page_not_reached = False
                    break

        with open("Check_villes_after_dossen.txt","a",encoding ="utf-8") as file :
            print(ville+"\t"+str(check_ville),file = file)

    except Exception as e :
        with open('Exception.txt','a',encoding="utf-8") as fhandle:
            print(ville+" : "+str(e),file=fhandle)
        chrome.quit()
    chrome.quit()
 


#Main crawl fonction
def main() :
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
       	future_to_url = {executor.submit(crawl, url): url for url in Villes}
       	for future in tqdm(concurrent.futures.as_completed(future_to_url),total=len(future_to_url)):
       		url = future_to_url[future]
       		try:
       			data = future.result()
       		except Exception as exc:
       			with open('exception.txt',"a") as flog:
       				print('%r generated an exception: %s' % (url, exc),file=flog)
       		else:
       			with open('completed.txt',"a") as flog:
       				print('%r page is completed' % url,file=flog)
                     
if __name__ == "__main__":
    main()
