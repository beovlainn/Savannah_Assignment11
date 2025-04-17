#File Name : cleaner.py
#Student Name: Ian McDaniel, Evan Bolin
#email:  mcdaniip@mail.uc.edu, bolinen@mail.uc.edu
#Assignment Number: Assignment 11
#Due Date:   April 17 2025
#Course #/Section:   IS4010-001
#Semester/Year:   Spring 2025
#Brief Description of the assignment:   Using api and python to clean up a csv file and create two new ones 
#Brief Description of what this module does.: Furthering our knowledge of the python language with api usage and csv experience 
#Citations: https://chatgpt.com

import csv
import re
import requests
from zip_lookup.zipcode_fixer import *


class DataCleaner:
    def __init__(self, input_path, cleaned_path, anomaly_path):
        """
        Initializes the DataCleaner with input, output, and anomaly CSV paths.
        @param input_path: Path to input CSV
        @param cleaned_path: Path to output cleaned CSV
        @param anomaly_path: Path to anomaly CSV output
        """
        self.input_path = input_path
        self.cleaned_path = cleaned_path
        self.anomaly_path = anomaly_path
        self.zip_lookup = ZipLookup()

    def clean_data(self):
        """
        Cleans the data by removing duplicates, handling missing ZIP codes,
        formatting prices, and logging anomalies.
        @return: None
        """
        print("Starting data cleaning process...")
        seen_rows = set()
        cleaned_rows = []
        anomalies = []
        zip_fills_done = 0
        max_lookups = 5

        with open(self.input_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            if "City" not in fieldnames:
                fieldnames += ["City", "State", "Zip"]
            data = list(reader)

        for row in data:
            try:
                row_key = tuple(row.items())
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)

                fuel_type = (row.get("Fuel Type") or "").strip().lower()
                if "pepsi" in fuel_type:
                    anomalies.append(row)
                    continue

                full_address = row.get("Full Address", "")
                city, state, zip_code = self._parse_address(full_address)

                existing_zip = (row.get("Zip") or "").strip()
                if not existing_zip or existing_zip in ["0", "00000"]:
                    if not zip_code and city and state and zip_fills_done < max_lookups:
                        print(f"Looking up ZIP for: {city}, {state}")
                        zip_code = self.zip_lookup.lookup_zip(city, state)
                        if zip_code:
                            print(f"ZIP found for {city}: {zip_code}")
                            zip_fills_done += 1
                        else:
                            print(f"No ZIP found for {city}. Setting to 00000")
                            zip_code = "00000"
                    elif not zip_code:
                        zip_code = "00000"
                else:
                    zip_code = existing_zip

                row["City"] = city
                row["State"] = state
                row["Zip"] = str(zip_code).zfill(5)

                try:
                    gross_price = float(row.get("Gross Price", 0))
                    row["Gross Price"] = f"{gross_price:.2f}"
                except Exception:
                    row["Gross Price"] = "0.00"

                cleaned_rows.append(row)

            except Exception as e:
                print(f"Error processing row: {row}\nException: {e}")
                continue

        print(f"Writing cleaned data to: {self.cleaned_path}")
        self._write_csv(self.cleaned_path, fieldnames, cleaned_rows)
        print(f"Writing anomalies to: {self.anomaly_path}")
        self._write_csv(self.anomaly_path, fieldnames, anomalies)
        print(f"Cleaning complete. ZIPs filled using API: {zip_fills_done}")

    def _parse_address(self, address):
        """
        Attempts to extract city, state, and ZIP from a given address string.
        @param address: Full address string
        @return: (city, state, zip_code) tuple
        """
        try:
            match = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5})?', address)
            if match:
                city = match.group(1).strip()
                state = match.group(2).strip()
                zip_code = match.group(3).strip() if match.group(3) else ""
                return city, state, zip_code
        except Exception:
            pass
        return "", "", ""

    def _write_csv(self, path, fieldnames, rows):
        """
        Writes rows of data to a CSV file.
        @param path: Output file path
        @param fieldnames: List of column headers
        @param rows: List of dictionaries to write
        @return: None
        """
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


class ZipLookup:
    def __init__(self):
        """
        Initializes the ZIP lookup service with API endpoints and builds city-ZIP map.
        """
        self.api_key = "0505edc0-1b37-11f0-8166-a9d411f63eef"
        self.radius_endpoint = "https://app.zipcodebase.com/api/v1/radius"
        self.search_endpoint = "https://app.zipcodebase.com/api/v1/search"
        self.anchor_zip = "45255"
        self.radius = 50
        print(f"Building ZIP code map for cities within {self.radius} miles of {self.anchor_zip}...")
        self.city_zip_map = self._build_city_zip_map()
        self.lookup_count = 0
        self.max_lookups = 5

    def _build_city_zip_map(self):
        """
        Builds a map of city names to ZIP codes by querying the radius and search endpoints.
        @return: Dictionary mapping city names to ZIP codes
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

            print(f"Found {len(zip_codes)} ZIP codes in radius of {self.radius} miles.")

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

        print(f"City-ZIP map built with {len(city_zip_map)} entries.\n")
        return city_zip_map

    def _chunk(self, data_list, size):
        """
        Splits a list into chunks of a specified size.
        @param data_list: List to split
        @param size: Max chunk size
        @return: Generator yielding chunks
        """
        for i in range(0, len(data_list), size):
            yield data_list[i:i + size]

    def lookup_zip(self, city, state):
        """
        Looks up a ZIP code for a given city using the prebuilt map.
        @param city: City name
        @param state: State abbreviation (not currently used)
        @return: ZIP code or None if not found or over lookup limit
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



