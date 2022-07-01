import argparse
import json
import logging
import requests
from collections.abc import MutableMapping

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
October 2020
"""


log = logging.getLogger(__name__)

class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass
    
    def flatten(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
        
    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        # TODO: implement - this dummy implementation returns one batch of data
        page = 0
        batch_count = 0
        begin_date = 20210301
        end_date = 20210301
        key = 'ofKOc4FAgiTt28Tg8aKNHo3yGMNWJrR4'
        output_list = []
        requestUrl = "https://api.nytimes.com/svc/search/v2/articlesearch.json?begin_date={}&end_date={}&facet=false&facet_filter=false&fl=headline,_id&page={}&api-key={}".format(begin_date, end_date, page, key)
        request = requests.get(requestUrl)
        response_dic = json.loads(request.content)
        doc_dic_list = response_dic['response']['docs']
        for line in doc_dic_list:
            if batch_count < batch_size:
                flattened_line = self.flatten(line)
                output_list.append(flattened_line)
                batch_count += 1
        # print(output_list)
        yield output_list
            
        
        # yield [
        #     {
        #         "headline.main": "The main headline",
        #         "_id": "1234",
        #     }
        # ]

    
    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema


if __name__ == "__main__":
    config = {
        "api_key": "NYTIMES_API_KEY",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")
