import scrapy
#import csv
import json
from urllib.parse import urlencode
import re
from scrapeops_python_requests.scrapeops_requests import ScrapeOpsRequests

scrapeops_logger =  ScrapeOpsRequests(
                    scrapeops_api_key="f03c00f3-5283-47a3-a76e-874e756a325e", 
                    spider_name="jobs",
                    job_name="Indeed"
                    )
requests = scrapeops_logger.RequestsWrapper()
custom_settings = {
    'FEEDS': { 'data/%(name)s_%(time)s.csv': { 'format': 'csv',}}
    }

#scrapy runspider jopspider.py
class JobSpider(scrapy.Spider):
    name = "jobs"
#    start_urls = ["https://www.indeed.com/jobs?q=data+scientist&l=Remote",]


    def get_indeed_search_url(self, keyword, location, offset=0):
        parameters = {"q": keyword, "l": location, "filter": 0, "start": offset}
        return "https://www.indeed.com/jobs?" + urlencode(parameters)


    def start_requests(self):
        keyword_list = ['data analyst']
        location_list = ['Remote']
        for keyword in keyword_list:
            for location in location_list:
                indeed_jobs_url = self.get_indeed_search_url(keyword, location)
                yield scrapy.Request(url=indeed_jobs_url, callback=self.parse_search_results, meta={'keyword': keyword, 'location': location, 'offset': 0})
    
    
    def parse_search_results(self, response):
        location = response.meta["location"]
        keyword = response.meta["keyword"]
        offset = response.meta["offset"]
        script_tag = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', response.text)
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])
            # for good pages, extract job data
            job_list = json_blob['metaData']['mosaicProviderJobCardsModel']['results']
            for index, job in enumerate(job_list):
                if job.get("jobkey") is not None:
                    job_url = 'https://www.indeed.com/viewjob?jk=' + job.get('jobkey')
                    yield scrapy.Request(
                        url=job_url,
                        callback=self.parse,
                        meta={
                                'keyword': keyword, 
                                'location': location, 
                                'page': round(offset / 10) + 1 if offset > 0 else 1,
                                'position': index,
                                'jobKey': job.get('jobkey'),
                            })
            # Paginate
            if offset == 0:
                meta_data = json_blob["metaData"]["mosaicProviderJobCardsModel"]["tierSummaries"]
                num_results = sum(category["jobCount"] for category in meta_data)
                if num_results > 1000:
                    num_results = 50
                
                for offset in range(10, num_results + 10, 10):
                    url = self.get_indeed_search_url(keyword, location, offset)
                    yield scrapy.Request(url=url, callback=self.parse_search_results, meta={'keyword': keyword, 'location': location, 'offset': offset})


    def parse(self, response):
        location = response.meta['location']
        keyword = response.meta['keyword'] 
        page = response.meta['page'] 
        position = response.meta['position'] 
        script_tag  = re.findall(r"_initialData=(\{.+?\});", response.text)
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])
            job = json_blob["jobInfoWrapperModel"]["jobInfoModel"]
            yield {
                #'debug' : response.text,
                'keyword': keyword,
                'location': location,
                'page': page,
                'position': position,
                'company': job["jobInfoHeaderModel"].get('companyName'),
                'jobkey': response.meta['jobKey'],
                'jobTitle': job["jobInfoHeaderModel"].get('jobTitle'),
                'salarymin' : job["jobInfoHeaderModel"].get("salaryMin"),
                'salarymax' : job["jobInfoHeaderModel"].get("salaryMax"),
                'salarytype' : job["jobInfoHeaderModel"].get("salaryType"),
                'post_age' : json_blob["jobMetadataFooterModel"].get("age"),
                'jobDescription': job.get('sanitizedJobDescription').get('content') if job.get('sanitizedJobDescription') is not None else '',
            }
    # def parse(self, response):
    #     with open("jobs.csv", "w") as f:
    #         writer = csv.DictWriter(f, fieldnames=["title", "company", "location", "salary", "requirements"])
    #         writer.writeheader()
    #         for job in response.css('div.jobsearch-SerpJobCard'):
    #             writer.writerow({
    #                 "title" : job.css('a.jobtitle::text').get(),
    #                 "company" : job.css('span.company::text').get(),
    #                 "location" : job.css('div.location::text').get(),
    #                 "salary" : job.css('span.salaryText::text').get(),
    #                 "requirements" : job.css('div.jobCardSummary ul li::text').getall()
    #             })

    #     nextpage = response.css('div.pagination a.np::attr(href)').get()
    #     if not nextpage is None:
    #         yield response.follow(nextpage, self.parse)
