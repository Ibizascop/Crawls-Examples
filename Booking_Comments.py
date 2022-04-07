# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 22:42:27 2022

@author: ibiza
"""

# An example on how to crawl comments on hotels on the website Booking.com

import os
import time
import re
import random
import concurrent.futures
import warnings
import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from tqdm import tqdm

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor, as_completed

#Ignore Depreciation warnings
warnings.filterwarnings("ignore",category = DeprecationWarning)

#Get hotels urls
try :
    with open("log.txt","r",encoding='utf-8') as log :
        done_urls = log.readlines()
        done_urls = [url.replace("\n","") for url in done_urls]
        log.close()
except Exception as e:
        pass

with open(r"Landes_urls.txt","r") as liste_hotels :
    liste_urls = liste_hotels.readlines()
    liste_urls = [url.replace("\n","") for url in liste_urls]
    liste_hotels.close()

try :
    liste_urls = [url for url in liste_urls if url not in done_urls]
except Exception as e:
    pass


with open('exception.txt',"w") as flog:
    print(datetime.datetime.now(),'\n')

def loop(filename_url,url, hotel_name) :
    #Loop on one page of comments
    for i in range(1,11) :
        try :
            note = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[2]/div[1]/div/div[2]/div/div'.format(i))
            note = note.text
        except Exception as e:
            note = ""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)
        try :
            chambre = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[1]/div[2]/ul/li/a/div'.format(i))
            chambre = chambre.text
        except Exception as e:
            chambre =""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)
        try :
            durée = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[1]/ul[1]/li/div'.format(i))
            durée_sejour = durée.text.split(" ")[0]
            mois_sejour = durée.text.split(" ")[-2]
            année_sejour = durée.text.split(" ")[-1]
        except Exception as e:
            durée_sejour = ""
            mois_sejour = ""
            année_sejour = ""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)
        try :
            voyageur = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[1]/ul[2]/li/div'.format(i))
            voyageur = voyageur.text
        except Exception as e:
            voyageur = ""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)

        try :
            natio = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[1]/div[1]/div/div[2]/span[2]'.format(i))
            natio = natio.text
        except Exception as e:
            natio = ""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)

        try :
            #Avoid taking other stuff than the date of the comment
            date = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[2]/div[1]/span[2]'.format(i))
            date = date.text.replace("Commentaire envoyé le ","")
        except Exception as e:
            try :
                date = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[2]/div[1]/span'.format(i))
                date = date.text.replace("Commentaire envoyé le ","")
            except:
                date = ""
                with open('exception.txt',"a") as flog:
                    print('%r generated an exception: %s' % (url, e),file=flog)

        try :
            titre = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[2]/div[1]/div/div[1]/h3[1]'.format(i))
            titre = titre.text
        except Exception as e:
            titre = ""
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)

        try :
            commentaire = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/ul/li[{}]/div/div[2]/div[2]/div[2]/div'.format(i))
            commentaire = commentaire.text.replace("\n","")
        except Exception as e:
           commentaire =""
           with open('exception.txt',"a") as flog:
               print('%r generated an exception: %s' % (url, e),file=flog)

        #
        result=(url+'\t'+ str(hotel_name).replace('\t','').replace('\n','')+ '\t'+ str(note).replace('\t','').replace('\n','')+ '\t'+ str(chambre).replace('\t','').replace('\n','')+'\t'
                +str(durée_sejour).replace('\t','').replace('\n','') +'\t' +str(mois_sejour).replace('\t','').replace('\n','') +'\t' +str(année_sejour).replace('\t','').replace('\n','')
                +'\t' +str(voyageur).replace('\t','').replace('\n','')+'\t' +str(natio).replace('\t','').replace('\n','')+'\t' +str(date).replace('\t','').replace('\n','') +'\t' +str(titre).replace('\t','').replace('\n','')
                +'\t' +str(commentaire).replace('\t','').replace('\n',''))

        with open(r".\AVIS\{}.csv".format(filename_url),'a',encoding='utf-8') as file:
            print(result,file = file)

#Main function
def crawl_hotel_comments(url) :
    global chrome
    try :
        #Making sure all drivers don't start at the same time
        time.sleep(random.uniform(1,5))

        timestamp = round(time.time()*1000000)

        filename_url = str(timestamp) + str(re.findall(r'fr\/(.+).fr.html',url)[0])

        #File to save results
        with open(r".\AVIS\{}.csv".format(filename_url),"w",encoding='utf-8') as file :
            print('Urls\tHotels\tNotes\tTypes_chambres\tDurées_sejours\tMois_sejours\tAnnees_sejours'+
                  '\tTypes_voyageurs\tNationalites\tDates_commentaires\tTitres_commentaires\tCommentaires',file=file)


        chrome_options = Options()
        path = r"C:\Cours\Python\chromedriver_win32\chromedriver.exe"
        #chrome_options.add_argument("--headless")
        chrome = webdriver.Chrome(executable_path= path ,options=chrome_options)
        chrome.maximize_window()
        chrome.get(url)

        time.sleep(2)
        #chrome.save_screenshot(str(filename_url)+'1.png')
        #Nom hotel
        hotel_name = chrome.find_element_by_xpath('//*[@id="hp_hotel_name"]')
        hotel_name = hotel_name.text
        
        try :
            nb_commentaires = chrome.find_element_by_xpath('//*[@id="show_reviews_tab"]/span')
            nb_commentaires = nb_commentaires.text
            nb_commentaires = re.search("(\d+)",nb_commentaires).group()
        except:
            nb_commentaires =''
        with open('Check_nb_commentaires.txt',"a") as flog:
            print(url+"\t"+nb_commentaires,file=flog)
        #Acceder commentaires
        try :
            btn_avis = chrome.find_element_by_xpath('//*[@id="show_reviews_tab"]')
            chrome.execute_script("arguments[0].scrollIntoView();", btn_avis)
            chrome.execute_script("arguments[0].click();",btn_avis)
            time.sleep(3)
        except Exception as e :
            with open('exception.txt',"a") as flog:
                print('%r generated an exception: %s' % (url, e),file=flog)
        #chrome.save_screenshot(str(filename_url)+'2.png')


        #Loop on all pages of comments
        #First page
        loop(filename_url,url, hotel_name)
        last_page_not_reached = True
        #Other pages
        while last_page_not_reached :
            try :
                popup = chrome.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]')
                chrome.execute_script("arguments[0].scrollIntoView();", popup)
                chrome.execute_script("arguments[0].click();",popup)
                time.sleep(3)
            except Exception as e :
                pass
            #chrome.save_screenshot(str(filename_url)+'3.png')
            i = 0
            btn = False
            while True :
                if btn == False and i <3:
                    i += 1
                    try :
                        new_page = chrome.find_element_by_xpath('//*[@id="review_list_page_container"]/div[4]/div/div[1]/div/div[3]/a')
                        chrome.execute_script("arguments[0].scrollIntoView();", new_page)
                        chrome.execute_script("arguments[0].click();",new_page)
                        time.sleep(3)

                        btn = True
                        loop(filename_url,url, hotel_name)

                    except Exception as e:
                        with open('exception.txt',"a") as flog:
                            print('%r could not click next page btn: %s' % (url, e),file=flog)
                elif btn == True :
                    break
                elif i>=3:
                    with open('exception.txt',"a") as flog:
                        print('%r reached last page: ' % (url),file=flog)
                    last_page_not_reached = False
                    #chrome.save_screenshot(str(filename_url)+'4.png')
                    break

        with open("log.txt",'a',encoding='utf-8') as file:
            print(url,file = file)
        chrome.quit()
    except Exception as e:
        with open('exception.txt',"a") as flog:
            print('%r generated an exception: %s' % (url, e),file=flog)
        chrome.quit()

#Multithreading
def main() :
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
       	future_to_url = {executor.submit(crawl_hotel_comments, url): url for url in liste_urls}
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
