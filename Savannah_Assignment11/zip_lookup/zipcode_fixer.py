#File Name : zipcode_fixer.py
#Student Name: Ian McDaniel, Evan Bolin
#email:  mcdaniip@mail.uc.edu, bolinen@mail.uc.edu
#Assignment Number: Assignment 11
#Due Date:   April 17 2025
#Course #/Section:   IS4010-001
#Semester/Year:   Spring 2025
#Brief Description of the assignment:   Using api and python to clean up a csv file and create two new ones 
#Brief Description of what this module does.: Furthering our knowledge of the python language with api usage and csv experience 
#Citations: https://chatgpt.com

import requests

class ZipLookup:
    """
    A class that provides ZIP code lookup functionality using the Zipcodebase API.
    It builds a city-to-ZIP mapping within a specified radius of an anchor ZIP code
    and allows a limited number of lookups using that mapping.
    """

    def __init__(self):
        """
        Initializes the ZipLookup instance with API keys and endpoints,
        and pre-builds a map of city names to ZIP codes based on ZIPs
        within a certain radius from a specified anchor ZIP.
        """
        self.api_key = "b381ffb0-1a53-11f0-b498-9b538c7eca4b"
        self.radius_endpoint = "https://app.zipcodebase.com/api/v1/radius"
        self.search_endpoint = "https://app.zipcodebase.com/api/v1/search"
        self.anchor_zip = "45255"
        self.radius = 50
        self.city_zip_map = self._build_city_zip_map()
        self.lookup_count = 0
        self.max_lookups = 5
        #finds first 5 zips from API and supplements into the new csv file 

    def _build_city_zip_map(self):
        """
        Builds a mapping of city names to ZIP codes for ZIPs located within
        a defined radius of the anchor ZIP. Uses the Zipcodebase radius and
        search endpoints.

        @return: Dictionary where keys are lowercase city names and values are ZIP codes
        """
        city_zip_map = {}
        try:
            radius_params = {
                "apikey": self.api_key,
                "code": self.anchor_zip,
                "radius": self.radius,
                "country": "us"
            }
            radius_response = requests.get(self.radius_endpoint, params=radius_params)
            radius_response.raise_for_status()
            radius_data = radius_response.json()
            zip_codes = [item["code"] for item in radius_data.get("results", [])]

            for zip_chunk in self._chunk(zip_codes, 10):
                search_params = {
                    "apikey": self.api_key,
                    "codes": ",".join(zip_chunk),
                    "country": "us"
                }
                search_response = requests.get(self.search_endpoint, params=search_params)
                search_response.raise_for_status()
                search_data = search_response.json()

                results = search_data.get("results", {})
                for zip_code, info_list in results.items():
                    if info_list:
                        city_name = info_list[0].get("city")
                        if city_name and zip_code:
                            city_zip_map[city_name.lower()] = zip_code

        except Exception as e:
            print(f"Error building city-ZIP map: {e}")

        return city_zip_map

    def _chunk(self, data_list, size):
        """
        Yields successive chunks from a list.

        @param data_list: List to break into chunks
        @param size: Maximum size of each chunk
        @return: Generator yielding chunks of data_list
        """
        for i in range(0, len(data_list), size):
            yield data_list[i:i + size]

    def lookup_zip(self, city, state):
        """
        Looks up the ZIP code for a given city using the internal mapping.
        Limits the number of API-based lookups to a configured maximum.

        @param city: Name of the city
        @param state: Name or abbreviation of the state (currently unused)
        @return: ZIP code as string if found, else None
        """
        if self.lookup_count >= self.max_lookups:
            return None
        if not city:
            return None

        zip_code = self.city_zip_map.get(city.lower())
        if zip_code:
            self.lookup_count += 1
            return zip_code
        return None