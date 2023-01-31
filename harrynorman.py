import requests
from parsel import Selector
import json
from pipelines import HarryPipeline
from settings import *

class Harry:
   
      
    def parse_link(self, url):
            response = requests.get(url=url, headers=headers)
            res = Selector(text=response.text)
            next=res.xpath('//li[@class="page-item next"]/a/@href').extract_first()
            link=res.xpath("//a[@class='agent-link']/@href").extract()
            for url in link:
                url=f'https://www.harrynorman.com{url}'
                self.parse(url)
            url=f'https://www.harrynorman.com{next}'
            self.parse_link(url)
    def parse(self, url):
        response= requests.get(url=url, headers=headers)
        if response.status_code!=200:
            print("error")
        res = Selector(text=response.text)
        
        #fields
        first_name=""
        middle_name=""
        last_name=""
        office_name=""
        address1=""
        city=""
        state=""
        zipcode=""
        image_url=""
        country="United States"
        title=""
        email=""
        profile_url=url
        website=""
        office_phone_numbers=[]
        agent_phone_numbers=[]
        linkedin=""
        facebook=""
        twitter=""
        description=""
        languages=[]
        
        #Xpath
        
        NAME_XPATH="//h1[@class='body-title']/text()"
        EMAIL_XPATH="//a[@class='agent_email']/text()"
        OFFICE_NAME_XPATH="//span[@class='agent-office-name']/text()"
        ADDRESS_XPATH1="//div[@class='line-height-reset']/text()"
        ADDRESS_XPATH2="//div[contains(@class,'agent-summary')]/text()"
        AGENT_PHONE_NUMBERS_XPATH="//span[contains(text(),'PHONE:')]/following-sibling::a/text()"
        IMAGE_XPATH="//img[@class='agent-photo']/@src"
        WEBSITE_XPATH="//a[contains(text(),'MY WEBSITE')]/@href"
        DESCRIPTION_XPATH='//div[@class="col-sm-24"]//p/text()'
        LANGUAGES_XPATH="//ul[@class='first']/li/text()"
        SOCIAL_XPATH="//div[@class='agent-social-icons social']//a/@href"
        TITLE_XPATH="//span[@class='agent-office-name']/following-sibling::div//h3/text()"
        OFFICE_PHONE_NUMBERS_XPATH="//span[contains(text(),'OFFICE:')]/following-sibling::a/text()"
        
        #EXTRACT
        
        name=res.xpath(NAME_XPATH).extract_first()
        email=res.xpath(EMAIL_XPATH).extract_first()
        office_name=res.xpath(OFFICE_NAME_XPATH).extract_first().strip()
        address=res.xpath(ADDRESS_XPATH1).extract()
        address2=res.xpath(ADDRESS_XPATH2).extract()
        agent_phone_numbers=res.xpath(AGENT_PHONE_NUMBERS_XPATH).extract()
        image_url=res.xpath(IMAGE_XPATH).extract_first()
        website=res.xpath(WEBSITE_XPATH).extract_first()
        description=res.xpath(DESCRIPTION_XPATH).extract()
        languages=res.xpath(LANGUAGES_XPATH).extract()
        title=res.xpath(TITLE_XPATH).extract_first()
        office_phone_numbers=res.xpath(OFFICE_PHONE_NUMBERS_XPATH).extract()
        other_urls=res.xpath(SOCIAL_XPATH).extract()
       

        
        #Cleaning
        
        if " " in name:
             name=name.split(" ")
             if len(name)==2:
                first_name = name[0]
                middle_name = ''
                last_name = name[1]
             elif len(name)==3:
                  first_name = name[0]
                  middle_name = name[1]
                  last_name = name[2]
             else:
                 name=''.join(name)
                 first_name = name
                 middle_name = ''
                 last_name = ''
        else:
            first_name = name
            middle_name = ''
            last_name = ''
            
            
            
        others=[]
        for url in other_urls:
            if url != None and "linkedin" in url:
               linkedin=url
            elif url != None and "facebook" in url:
                facebook=url
            elif url != None and "twitter" in url:
                twitter=url
            else:
                others.append(url)
        if facebook == None:
            facebook = ""
        if linkedin == None:
            linkedin = ""
        if twitter == None:
            twitter = ""
        
            
        try:
            if len(address)==3:
                address1=address[0]+address[1]
                city=address[2].split(",")[0]
                zip_state=address[2].split(",")[1].strip(" ")
                zip_state=zip_state.split(" ")
                state=zip_state[0]
                zipcode=zip_state[1]
            else:
                address1=address[0]
                city=address[1].split(",")[0]
                zip_state=address[1].split(",")[1].strip(" ")
                zip_state=zip_state.split(" ")
                state=zip_state[0]
                zipcode=zip_state[1]
        except:
            address2=' '.join(address2).strip().split(",")
            city=address2[0].split(" ")[-1]
            address1=address2[0].split(" ")
            address1.remove(city)
            address1=' '.join(address1)
            zip_state=address2[1].strip().split(" ")
            state=zip_state[0]
            zipcode=zip_state[1]
         
       
        if title != None:
            title = title.strip()
        else:
            title=""
        if description != None:
            description=' '.join(description).strip().replace("\n"," ").replace("\r","")
           
            
        
        features = {
            "first_name":first_name,
            "middle_name":middle_name,
            "last_name":last_name,
            "office_name":office_name,
            "address":address1,
            "city":city,
            "state":state,
            "zipcode":zipcode,
            "image_url":image_url,
            "country":country,
            "title":title,
            "email":email,
            "profile_url":profile_url,
            "website":website,
            "office_phone_numbers":office_phone_numbers,
            "agent_phone_numbers":agent_phone_numbers,
            "social" : {
			'facebook_url': facebook,
			'linkedin_url': linkedin,
			'twitter_url': twitter,
			'other_urls': others
              },
            "description":description,
            "languages":languages
            
        }
        print(json.dumps(features, indent=2))
        db=HarryPipeline()
        db.process_item(features)
       
            
            
url = "https://www.harrynorman.com/agents/"
exp = Harry()
exp.parse_link(url)
