import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from scraper_api import ScraperAPIClient
import urllib3
from csv import reader
import concurrent.futures

CONNECTIONS=10
linkorder=1
urls=[]
with open('urls.csv', 'r') as f:
    csv_reader = reader(f)
    next(csv_reader)
    for row in csv_reader:
        urls.append((row ,linkorder))
        linkorder+=1
print(len(urls))

client=ScraperAPIClient('')

#headers={'User-Agent':uastring, "Accept-Encoding": "*","Connection": "keep-alive"}

x=0
counter=0
datalist=[]
def get_data(url):
    global counter
    global x
    while True:
        try:
            r=client.get(url[0])
            soup=BeautifulSoup(r.text, 'html.parser')

                    #check for page loading
            while soup.find('div',{'class':'col-left'})!=None or r.status_code==404:

                r=client.get(url[0])
                soup=BeautifulSoup(r.text, 'html.parser')
                print("error... retrying")
                        #print(r.status_code)
                        # get data
            eldata=soup.find('dl',{'class':'attributes dl-horizontal'})

            elcmpny_no=eldata.find('dd',{'class':'company_number'})
                    #print(elcmpny_no)
            if elcmpny_no != None:
                cmpny_no=str(elcmpny_no.text)
            else:
                cmpny_no=' '
                            #print('got cmpny no')
            elestatus=eldata.find('dd',{'class':'status'})
            if elestatus != None:
                elstatus=elestatus.text
            else:
                elstatus=' '
                        #print('got status')
            elincorporationdate=eldata.find('dd',{'class':'incorporation_date'})
            if elincorporationdate != None:
                incorporationdate=elincorporationdate.text
            else:
                incorporationdate=' '
                        #print('got incorp. date')
            elcmpnytypekolo=eldata.find('dd',{'class':'company_type'})
            if elcmpnytypekolo != None:
                elcmpnytype=elcmpnytypekolo.text
            else:
                elcmpnytype=' '
                        #print('got cmpny type')
            eljurisdictionkolo=eldata.find('dd',{'class':'jurisdiction'})
            if eljurisdictionkolo != None:
                eljurisdiction=eljurisdictionkolo.text
            else:
                eljurisdiction=' '
                        #print('got jurisdiction')
            eladdrslist=[]
            eladressparent=eldata.find('ul',{'class':'address_lines'})
            if eladressparent != None:
                for addrs in eladressparent.findChildren('li'):
                    eladdrslist.append(addrs.text)

                eladdrsbgd=' '.join([str(item) for item in eladdrslist])
            else:
                eladdrsbgd=' '
                        #print('got addrs')
            elagentname=eldata.find('dd',{'class':'agent_name'})

            if elagentname != None:
                elagent=elagentname.text
            else:
                elagent=' '

                        #print('got agent')
            elagentaddresskolo=eldata.find('dd',{'class':'agent_address'})
            if elagentaddresskolo != None:
                elagentaddress= elagentaddresskolo.text
            else:
                elagentaddress=' '
                        #print('got agent adrs')
            eloffcrelkber=eldata.find('dd',{'class':'officers'})

            if eloffcrelkber!=None:
                eloffcrsparent=eloffcrelkber.find('ul',{'class':'attribute_list'})
                if eloffcrsparent!= None:
                    offcrslist=[]
                    for offcr in eloffcrsparent.findChildren('li'):
                        offcrslist.append(offcr.text)

                    eloffcrsbgd=', '.join([str(officr) for officr in offcrslist])
                else:
                    eloffcrsbgd=' '
            else:
                eloffcrsbgd=' '

                        #print('got ofcrs')
            elrgstrykolha=eldata.find('dd',{'class':'registry_page'})
            if elrgstrykolha != None :
                try:
                    rgstrylink=elrgstrykolha.find('a',{'class':'url external'})['href']
                except TypeError:
                    rgstrylink=' '
            else:
                rgstrylink=' '
                        #print('got rgstry')



            datapoints={'order':url[1],'url':url[0], 'company number': cmpny_no, 'status':elstatus, 'Incorp date':incorporationdate, 'company type':elcmpnytype, 'jurisdiction':eljurisdiction,'address':eladdrsbgd, 'agent':elagent, 'agent address':elagentaddress, 'officer':eloffcrsbgd,'registry':rgstrylink}


            counter+=1
            print(counter)


                        #print('datapoints done')
                    #except requests.exceptions.MissingSchema:
                        #datapoints= {'company number': ' ', 'status':' ', 'Incorp date':' ', 'company type':' ', 'jurisdiction':' ','address':' ', 'agent':' ', 'agent address':' ', 'officer':' ','registry':' '}
                        #datalist.append(datapoints)
                        #counter+=1
                        #print(counter)
                        #time.sleep(random.randint(1,11))
            break

        except AttributeError:

            print("AttributeError...retrying")
            pass
        except requests.exceptions.ConnectionError:
            if x>200:
                print("error in link: {}".format(url))
                datapoints={'order':" ",'url':" ", 'company number': " ", 'status':" ", 'Incorp date':" ", 'company type':" ", 'jurisdiction':" ",'address':" ", 'agent':" ", 'agent address':" ", 'officer':" ",'registry':" "}
                break
            else:
                print("retrying to connect in 2 mins or will be saving...")
                time.sleep(1)
                print("error {}".format(x))
                x+=1
                pass


        except requests.exceptions.ChunkedEncodingError:
            print("Chuncked encoding error, will try again/continue in a while")
            time.sleep(random.randint(1,3))
            pass
        except requests.exceptions.HTTPError:
            print("http error")
            time.sleep(random.randint(1,3))
            pass
        except requests.exceptions.RetryError:
            print("retry error")
            time.sleep(random.randint(1,3))
            pass
        except urllib3.exceptions.MaxRetryError:
            time.sleep(random.randint(1,3))
            pass




    return datapoints



with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    results=executor.map(get_data, urls)

    for result in results:
        while True:
            try:
                datalist.append(result)
                break
            except:
                pass


df=pd.DataFrame(datalist)
df.to_csv(r'', mode='a', index=False, header=False )
